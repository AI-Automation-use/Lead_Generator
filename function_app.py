import logging
import azure.functions as func
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import sys
import asyncio
from openai import AzureOpenAI
from docx import Document 
from newspaper import Article
from urllib.parse import quote
import msal
import requests
import base64
import re
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO
import io
from azure.identity import DefaultAzureCredential

load_dotenv()

# Constants for authentication
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
TOKEN_FILE = "token.json"
SCOPES = ["Mail.Send"]
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

# Accessing the values from the .env file
API_KEY = os.getenv("API_KEY")
CX = os.getenv("CX")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
LEAD_EXCEL_CONTAINER_NAME="potentiallist"
LEAD_EXCEL_BLOB_NAME = "leads_tracking.xlsx" 

# Set up Azure OpenAI API
client = AzureOpenAI(
    api_key=AZURE_API_KEY,
    api_version="2024-12-01-preview",
    azure_endpoint=AZURE_ENDPOINT
)

# Ensuring Windows event loop policy for Playwright
# NOTE: This part is for Windows local development and can be removed for Linux-based
# Azure Function deployments, but it won't cause an error if left in.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# GNews API Configuration for fetching news
BASE_URL = "https://gnews.io/api/v4/search"


def get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def download_excel_from_blob(blob_service_client, container_name, blob_name):
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        return io.BytesIO(download_stream.readall())
    except Exception as e:
        print(f"Error downloading blob {blob_name}: {e}")
        return None

def get_identified_leads_df():
    blob_service_client = get_blob_service_client()
    excel_data = download_excel_from_blob(blob_service_client, LEAD_EXCEL_CONTAINER_NAME, LEAD_EXCEL_BLOB_NAME)
    if excel_data:
        try:
            df = pd.read_excel(excel_data)
            return df
        except Exception as e:
            print(f"Error reading Excel from blob: {e}")
            return pd.DataFrame(columns=["Company Name", "Lead Identification Areas", "Timestamp"])
    else:
        return pd.DataFrame(columns=["Company Name", "Lead Identification Areas", "Timestamp"])

def normalize_areas_string(areas_str):
    if not isinstance(areas_str, str):
        return ""
    parts = areas_str.replace(';', ',').split(',')
    cleaned_parts = sorted(list(set(area.strip() for area in parts if area.strip())))
    return ", ".join(cleaned_parts)

def add_lead_to_excel(company_name, lead_areas):
    blob_service_client = get_blob_service_client()
    df = get_identified_leads_df()
    normalized_incoming_areas_str = normalize_areas_string(lead_areas)
    incoming_areas_set = set(normalized_incoming_areas_str.split(', ') if normalized_incoming_areas_str else set())
    existing_company_row = df[df["Company Name"] == company_name]
    email_should_be_sent = False
    if not existing_company_row.empty:
        existing_areas_str = existing_company_row["Lead Identification Areas"].iloc[0]
        existing_areas_set = set(existing_areas_str.split(', ') if existing_areas_str else set())
        truly_new_areas = incoming_areas_set - existing_areas_set
        if truly_new_areas:
            updated_areas_set = existing_areas_set.union(incoming_areas_set)
            updated_areas_list = sorted(list(updated_areas_set))
            updated_areas_str = ", ".join(updated_areas_list)
            df.loc[df["Company Name"] == company_name, "Lead Identification Areas"] = updated_areas_str
            df.loc[df["Company Name"] == company_name, "Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            output = io.BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            upload_excel_to_blob(blob_service_client, LEAD_EXCEL_CONTAINER_NAME, LEAD_EXCEL_BLOB_NAME, output.getvalue())
            print(f"Company '{company_name}' updated with new lead areas: {', '.join(sorted(list(truly_new_areas)))}.")
            email_should_be_sent = True
        else:
            print(f"Company '{company_name}' already exists with these lead areas. No update or email needed.")
            email_should_be_sent = False
    else:
        new_entry = pd.DataFrame([{
            "Company Name": company_name,
            "Lead Identification Areas": normalized_incoming_areas_str,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }])
        df = pd.concat([df, new_entry], ignore_index=True)
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        upload_excel_to_blob(blob_service_client, LEAD_EXCEL_CONTAINER_NAME, LEAD_EXCEL_BLOB_NAME, output.getvalue())
        print(f"New company '{company_name}' added as a lead with areas: {normalized_incoming_areas_str}.")
        email_should_be_sent = True
    return email_should_be_sent

def fetch_full_article_text_with_playwright(page, url):
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        content = page.locator("body").inner_text()
        clean_text = content.strip().replace('\n', ' ').replace('\r', ' ')
        return clean_text[:10000] if clean_text else "⚠️ Full article not available."
    except Exception as e:
        print(f"Error fetching full article from {url}: {e}")
        return "⚠️ Full article not available."

def scrape_google_news(company_name, pages=1):
    query = quote(company_name)
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        for i in range(pages):
            start = i * 10
            url = f"https://www.google.com/search?q={query}&tbm=nws&start={start}"
            page.goto(url, wait_until="load", timeout=60000)
            soup = BeautifulSoup(page.content(), "html.parser")
            for result in soup.select("div.SoaBEf"):
                try:
                    a_tag = result.find("a", href=True)
                    link = a_tag["href"] if a_tag else ""
                    title_el = a_tag.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
                    description_el = a_tag.select_one("div.GI74Re.nDgy9d")
                    publisher_el = a_tag.select_one("span.xQ82C.e8fRJf")
                    date_el = result.select_one("span[class]:not([class*='xQ82C'])")
                    title = title_el.get_text(strip=True) if title_el else ""
                    short_description = description_el.get_text(strip=True) if description_el else ""
                    publisher = publisher_el.get_text(strip=True) if publisher_el else ""
                    published_on = date_el.get_text(strip=True) if date_el else ""
                    full_article = fetch_full_article_text_with_playwright(page, link)
                    if title and link and not any(r["url"] == link for r in results):
                        results.append({
                            "title": title,
                            "publisher": publisher,
                            "published_on": published_on,
                            "description": full_article or short_description,
                            "url": link
                        })
                except Exception:
                    continue
        browser.close()
    return results


def upload_excel_to_blob(blob_service_client, container_name, blob_name, data_stream):
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data_stream, overwrite=True)
        print(f"Successfully uploaded {blob_name} to blob storage.")
    except Exception as e:
        print(f"Error uploading blob {blob_name}: {e}")


# Constants for Blob Storage
TOKEN_CONTAINER_NAME = "potentiallist"
TOKEN_BLOB_NAME = "token.json"
SCOPES = ["https://graph.microsoft.com/.default"] # Use the .default scope for client credential flow

def get_access_token():
    logging.info("🔐 Acquiring access token...")
    token_cache = msal.SerializableTokenCache()
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
    
    # 1. Download the token cache from blob storage
    try:
        container_client = blob_service_client.get_container_client(TOKEN_CONTAINER_NAME)
        blob_client = container_client.get_blob_client(TOKEN_BLOB_NAME)
        download_stream = blob_client.download_blob()
        token_cache.deserialize(download_stream.readall().decode('utf-8'))
        logging.info("✅ Loaded token from blob storage cache.")
    except Exception as e:
        logging.warning(f"⚠️ Failed to load token cache from blob. Initial authentication may be required: {e}")
        # Note: This is where a local interactive session is needed to create the initial token.json.
        # This code block will fail in a deployed Function App.
        # It's here for local testing only. After initial setup, the 'accounts' check below handles it.
    
    app = msal.PublicClientApplication(
        client_id=os.getenv("CLIENT_ID"),
        authority=f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}",
        token_cache=token_cache
    )
    
    accounts = app.get_accounts()
    
    # Use the refresh token from the cache (downloaded from blob)
    result = app.acquire_token_silent(SCOPES, account=accounts[0]) if accounts else None
    
    # If a silent token acquisition fails, it means the refresh token is expired or not present.
    # In a deployed function, this should NOT happen if the refresh token is valid.
    if not result:
        raise Exception("❌ Token acquisition failed silently. Refresh token may be invalid. Manual re-authentication is required.")
        
    if token_cache.has_state_changed:
        try:
            container_client = blob_service_client.get_container_client(TOKEN_CONTAINER_NAME)
            blob_client = container_client.get_blob_client(TOKEN_BLOB_NAME)
            blob_client.upload_blob(token_cache.serialize(), overwrite=True)
            logging.info("💾 Token cache updated and saved to blob storage.")
        except Exception as e:
            logging.error(f"❌ Failed to upload token cache to blob: {e}")
            
    if "access_token" not in result:
        raise Exception(f"❌ Token acquisition failed: {result.get('error_description')}")
        
    return result["access_token"]

def get_company_website(company_name, api_key, cx):
    search_url = f"https://www.googleapis.com/customsearch/v1?q={company_name}+company+site&key={api_key}&cx={cx}"
    response = requests.get(search_url)
    if response.status_code == 200:
        results = response.json()
        if 'items' in results:
            return results['items'][0]['link']
    return None

def scrape_website(website):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            ignore_https_errors=True
        )
        page = context.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        page.goto(website, wait_until="load", timeout=60000)
        page_content = page.content()
        soup = BeautifulSoup(page_content, 'html.parser')
        title = soup.title.string if soup.title else "No title found"
        paragraphs = soup.find_all('p')
        paragraphs_content = '\n'.join([para.get_text() for para in paragraphs])
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        headings_content = '\n'.join([heading.get_text() for heading in headings])
        full_content = f"**Title:** {title}\n\n"
        full_content += f"**Headings:**\n{headings_content}\n\n"
        full_content += f"**Paragraphs:**\n{paragraphs_content}\n\n"
        full_content += f"\n\n**Source:** {website}"
        browser.close()
        return full_content, website

def check_potential_lead(website_content, linkedin_content, news_content):
    combined_content = f"""
    You are a market‑intelligence assistant.  

    Analyze the following source materials (Website, LinkedIn, News) and produce a **text report** with these three sections:

    1. **Interest**  
       For each area below, state “Yes” or “No” and in one sentence explain why for "yes" along with source link or supoorting content if present. Do not give random websites/Content if not found

       New Business Development Areas:  
         - SFR150  
         - Zones  
         - DYN365  
         - AI  
         - AWS  

       Large Deal Areas:  
         - Cost Takeout  
         - Cloud Migration  
         - Data Migration  
         - Platform Migration  
         - SaaS  
         - GCC  
         - Partner with IT

    2. **Contacts**  
       List each contact found, with type (email or phone or name along with title) and the source (Website, LinkedIn, or News).

    3. **Evidence**  
       Under sub‑headings for Website, LinkedIn, and News, give the exact excerpt(s) that support the interest findings or the contact info you listed. Do not give random websites/Content if not found

       **Sources**  
        Website Content:  
        {website_content}

        LinkedIn Profile Content:  
        {linkedin_content}

        Recent News Articles:  
        {news_content}

    """
    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=[{"role": "user", "content": combined_content}],
    )
    lead_analysis = response.choices[0].message.content
    potential_lead_check = classify_lead(lead_analysis)
    return lead_analysis, potential_lead_check

def classify_lead(lead_analysis):
    prompt = f"""
    You are a proactive lead generation expert.

    Given the following lead analysis, determine if this company shows any sign—direct or indirect—of being a potential lead. Even a slight indication of interest, relevance, or alignment should result in "Yes".

    **Lead Analysis:**
    {lead_analysis}

    Answer strictly with "Yes" or "No".
    """
    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=[{"role": "user", "content": prompt}],
    )
    result = response.choices[0].message.content.strip()
    return result

def extract_lead_details(lead_analysis, company):
    prompt = f"""
    Given the following lead analysis for the company "{company}", extract and return the following in plain text format:
    
    1. Customer/Client Name
    2. Lead Identification Area(s) (e.g., Cloud Migration, Platform Modernization)
    3. Contact Information (such as phone numbers, email addresses, LinkedIn profiles, job titles)

    Format the output like this:
    
    **Customer/Client Name**: <value>
    **Lead Identification Area**: <value>
    **Contact Information**: <value>
    
    If a field is not found, just write "Not available".

    Lead Analysis:
    {lead_analysis}
    """
    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=[{"role": "user", "content": prompt}],
    )
    extracted = response.choices[0].message.content.strip()
    return extracted


def create_lead_docx(lead_analysis: str, company: str):
    doc = Document()
    doc.add_heading(f"Lead Analysis for {company}", 0)
    doc.add_paragraph(lead_analysis)
    
    doc_stream = BytesIO()
    doc.save(doc_stream)
    doc_stream.seek(0)
    
    filename = f"{company}_lead_analysis.docx"
    return filename, doc_stream

def create_full_docx(website_content: str, linkedin_content: str, news_content: str, company: str):
    doc = Document()
    doc.add_heading("Company Content and Analysis", 0)
    doc.add_heading("Company Website Content:", level=1)
    doc.add_paragraph(website_content)
    doc.add_heading("Company LinkedIn Profile Content:", level=1)
    doc.add_paragraph(linkedin_content)
    doc.add_heading("Recent News Articles:", level=1)
    doc.add_paragraph(news_content)
    
    doc_stream = BytesIO()
    doc.save(doc_stream)
    doc_stream.seek(0)
    
    filename = f"{company}_full_content.docx"
    return filename, doc_stream

def send_email(access_token, recipient_emails, subject, body, attachments=None):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    to_recipients = [{"emailAddress": {"address": mail}} for mail in recipient_emails]
    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body},
            "toRecipients": to_recipients,
            "attachments": []
        }
    }
    
    if attachments:
        for name, data_stream in attachments:
            encoded_content = base64.b64encode(data_stream.read()).decode("utf-8")
            message["message"]["attachments"].append({
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": name,
                "contentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "contentBytes": encoded_content
            })
    
    resp = requests.post(
        f"{GRAPH_API_ENDPOINT}/me/sendMail",
        headers=headers,
        json=message
    )
    if resp.status_code == 202:
        logging.info("✅ Email with attachments sent!")
        return True
    else:
        logging.error(f"❌ Failed to send: {resp.status_code} - {resp.text}")
        return False


def markdown_bold_to_html(text):
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

# def send_lead_data_to_api(lead_areas, account_name, lead_name, lead_doc_name, lead_doc_stream):
def send_lead_data_to_api(
    lead_areas: str,
    account_name: str,
    lead_name: str,
    lead_doc_name: str | None = None,
    lead_doc_stream: bytes | None = None,
) -> bool:
    """
    Sends identified lead data and the lead analysis file to the external API.
    """
    api_url = os.getenv("LEAD_API_URL")

    if not api_url:
        logging.error("Error: 'LEAD_API_URL' environment variable not found.")
        return False

    # The data to be sent as part of the form
    data = {
        "new_leadidentificationarea": lead_areas,
        "new_name": lead_name,
        "new_accountname": account_name,
        ('new_supportingdocuments', (lead_doc_name, io.BytesIO(lead_doc_stream), 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'))
    }

    # The 'files' parameter handles multipart/form-data.
    # We include both the regular fields and the file here.
    files = [
        ('new_leadidentificationarea', (None, data['new_leadidentificationarea'])),
        ('new_name', (None, data['new_name'])),
        ('new_accountname', (None, data['new_accountname']))
    ]

    try:
        logging.info("Attempting to send lead data and file to API...")
        response = requests.post(api_url, files=files)
        response.raise_for_status()
        logging.info("✅ Lead data and file successfully sent to API.")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to send lead data to API: {e}")
        if e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        return False

# working when only data is sent attachment is pending through form-data
# def send_lead_data_to_api(lead_areas, account_name, lead_name, lead_doc_name, lead_doc_stream):
#     """
#     Sends identified lead data and the lead analysis file to the external API.
#     """
#     api_url = os.getenv("LEAD_API_URL")

#     if not api_url:
#         logging.error("Error: 'LEAD_API_URL' environment variable not found.")
#         return False

#     # The data to be sent as part of the form
#     data = {
#         "new_leadidentificationarea": lead_areas,
#         "new_name": lead_name,
#         "new_accountname": account_name
#     }

#     # The 'files' parameter handles multipart/form-data.
#     # We include both the regular fields and the file here.
#     files = [
#         ('new_leadidentificationarea', (None, data['new_leadidentificationarea'])),
#         ('new_name', (None, data['new_name'])),
#         ('new_accountname', (None, data['new_accountname']))
#     ]

#     try:
#         logging.info("Attempting to send lead data and file to API...")
#         response = requests.post(api_url, files=files)
#         response.raise_for_status()
#         logging.info("✅ Lead data and file successfully sent to API.")
#         return True
#     except requests.exceptions.RequestException as e:
#         logging.error(f"❌ Failed to send lead data to API: {e}")
#         if e.response is not None:
#             logging.error(f"Response content: {e.response.text}")
#         return False

# def send_lead_data_to_api(lead_areas, account_name, lead_name):
#     """
#     Sends identified lead data to the external API, assuming it only needs the 3 fields.
#     """
#     api_url = os.getenv("LEAD_API_URL")
    
#     if not api_url:
#         logging.error("Error: 'LEAD_API_URL' environment variable not found.")
#         return False

#     data = {
#         "new_leadidentificationarea": lead_areas, # This comes from your AI analysis
#         "new_name": lead_name,
#         "new_accountname": account_name
#     }

#     try:
#         logging.info("Attempting to send simplified lead data to API...")
#         response = requests.post(api_url, json=data)
#         response.raise_for_status() 
#         logging.info("✅ Simplified lead data successfully sent to API.")
#         return True
#     except requests.exceptions.RequestException as e:
#         logging.error(f"❌ Failed to send lead data to API: {e}")
#         if e.response is not None:
#              logging.error(f"Response content: {e.response.text}")
#         return False


# The Function App and Timer Trigger decorator
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
TARGET_COMPANY1 = os.getenv("TARGET_COMPANY1")

# @app.timer_trigger(schedule="0 */5 * * * *", arg_name="computacenter", run_on_startup=False)
# def timer_trigger(computacenter: func.TimerRequest) -> None:
# @app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False)
# def timer_trigger(myTimer: func.TimerRequest) -> None:
@app.function_name(name="ComputaCenter")
@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def ComputaCenter(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow()

    if myTimer.past_due:
        logging.warning("⏰ Timer is past due!")

    logging.info(f"🕒 Python timer trigger function started at: {utc_timestamp}")

    company = TARGET_COMPANY1
    pages = 1
    my_account_name = "Computacenter India"
    my_lead_name = "Lead from Lead Generator Tool"

    # 1. GNews Fetch
    logging.info(f"🔍 Fetching GNews articles for: {company}")
    today = datetime.utcnow()
    frm = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to = today.strftime("%Y-%m-%d")
    news_results = []
    try:
        resp = requests.get(
            BASE_URL,
            params={"q": company, "from": frm, "to": to, "lang": "en", "token": GNEWS_API_KEY},
            verify=True
        )
        resp.raise_for_status()
        for art in resp.json().get("articles", []):
            news_results.append({
                "title": art.get("title", ""),
                "description": art.get("description", ""),
                "url": art.get("url", "")
            })
        logging.info(f"✅ GNews API returned {len(news_results)} articles.")
    except Exception as e:
        logging.error(f"❌ GNews API error: {e}")

    # 2. Google News Scrape
    logging.info("🌐 Scraping Google News...")
    try:
        google_news_results = scrape_google_news(company, pages)
        logging.info(f"✅ Google News scrape found {len(google_news_results)} articles.")
    except Exception as e:
        logging.error(f"❌ Google News scraping error: {e}")
        google_news_results = []

    all_news = news_results + google_news_results
    if not all_news:
        logging.warning("⚠️ No news results found; skipping.")
        return

    news_content = "\n\n".join(
        f"**Title:** {n['title']}\n**Description:** {n['description']}\n**URL:** {n['url']}"
        for n in all_news
    )

    # 3. Website Scraping
    logging.info("🌐 Finding and scraping company website...")
    website = get_company_website(company, API_KEY, CX)
    if not website:
        logging.error(f"❌ Could not find website for {company}.")
        return
    try:
        website_content, _ = scrape_website(website)
        logging.info("✅ Website scraped successfully.")
    except Exception as e:
        logging.error(f"❌ Website scraping error: {e}")
        return

    # 4. LinkedIn Scraping
    logging.info("🔗 Finding and scraping LinkedIn profile...")
    linkedin_url = get_company_website(company + " LinkedIn", API_KEY, CX)
    if not linkedin_url:
        logging.error(f"❌ Could not find LinkedIn profile for {company}.")
        return
    try:
        linkedin_content, _ = scrape_website(linkedin_url)
        logging.info("✅ LinkedIn scraped successfully.")
    except Exception as e:
        logging.error(f"❌ LinkedIn scraping error: {e}")
        return

    # 5. AI Lead Check
    logging.info("🤖 Performing AI-based lead analysis...")
    try:
        lead_analysis, potential_lead_check = check_potential_lead(
            website_content, linkedin_content, news_content
        )
        logging.info("✅ Lead analysis complete.")
    except Exception as e:
        logging.error(f"❌ OpenAI analysis error: {e}")
        return
    logging.info(f"🔍 Is '{company}' a potential lead? → {potential_lead_check}")

    if potential_lead_check.strip().lower() == "yes":
        logging.info("📌 Lead confirmed. Extracting lead details...")
        try:
            details = extract_lead_details(lead_analysis, company)
            logging.info("✅ Lead details extracted.")
        except Exception as e:
            logging.error(f"❌ Failed to extract lead details: {e}")
            return

        m = re.search(r"\*\*Lead Identification Area\*\*: (.+)", details)
        lead_areas = m.group(1).strip() if m else "Not available"

        try:
            email_flag = add_lead_to_excel(company, lead_areas)
            logging.info("✅ Excel updated successfully.")
        except Exception as e:
            logging.error(f"❌ Excel update error: {e}")
            email_flag = False

        if email_flag:
            try:
                lead_doc_name, lead_doc_stream = create_lead_docx(lead_analysis, company)
                lead_doc_bytes = lead_doc_stream.getvalue()
                full_doc_name, full_doc_stream = create_full_docx(
                    website_content, linkedin_content, news_content, company
                )
                logging.info("📄 DOCX files generated.")
            except Exception as e:
                logging.error(f"❌ Error generating DOCX files: {e}")
                return

            try:
                token = get_access_token()
                logging.info("🔐 Access token acquired.")
                email_body = (
                    f"<html><body><p>A potential lead has been identified for <strong>{company}</strong>.</p>"
                    + "".join(f"<p>{markdown_bold_to_html(line)}</p>" for line in details.splitlines())
                    + "<p>See attachments for full reports.</p></body></html>"
                )
                sent = send_email(
                    token,
                    ["vishnu.kg@sonata-software.com"],
                    f"Potential Lead Identified – {company}",
                    email_body,
                    attachments=[(lead_doc_name, lead_doc_stream), (full_doc_name, full_doc_stream)]
                )
                if sent:
                    logging.info("✅ Email sent with attachments.")
                    send_lead_data_to_api(lead_areas, my_account_name, my_lead_name, lead_file_name=lead_doc_name, lead_doc_bytes)
                    logging.info("📨 Lead data posted to external API.")
                else:
                    logging.warning("⚠️ Email not sent.")
            except Exception as e:
                logging.error(f"❌ Error in email or token process: {e}")
        else:
            logging.info("📝 No new lead areas identified; skipping email.")
    else:
        logging.info(f"🚫 '{company}' is not identified as a potential lead; skipping all downstream steps.")

    logging.info("✅ Lead generation cycle completed.")

TARGET_COMPANY2 = os.getenv("TARGET_COMPANY2")
@app.function_name(name="PennyMac")
@app.schedule(schedule="0 */7 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def PennyMac(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow()

    if myTimer.past_due:
        logging.warning("⏰ Timer is past due!")

    logging.info(f"🕒 Python timer trigger function started at: {utc_timestamp}")

    company = TARGET_COMPANY2
    pages = 1
    my_account_name = "PennyMac"
    my_lead_name = "Lead from Lead Generator Tool"

    # 1. GNews Fetch
    logging.info(f"🔍 Fetching GNews articles for: {company}")
    today = datetime.utcnow()
    frm = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to = today.strftime("%Y-%m-%d")
    news_results = []
    try:
        resp = requests.get(
            BASE_URL,
            params={"q": company, "from": frm, "to": to, "lang": "en", "token": GNEWS_API_KEY},
            verify=True
        )
        resp.raise_for_status()
        for art in resp.json().get("articles", []):
            news_results.append({
                "title": art.get("title", ""),
                "description": art.get("description", ""),
                "url": art.get("url", "")
            })
        logging.info(f"✅ GNews API returned {len(news_results)} articles.")
    except Exception as e:
        logging.error(f"❌ GNews API error: {e}")

    # 2. Google News Scrape
    logging.info("🌐 Scraping Google News...")
    try:
        google_news_results = scrape_google_news(company, pages)
        logging.info(f"✅ Google News scrape found {len(google_news_results)} articles.")
    except Exception as e:
        logging.error(f"❌ Google News scraping error: {e}")
        google_news_results = []

    all_news = news_results + google_news_results
    if not all_news:
        logging.warning("⚠️ No news results found; skipping.")
        return

    news_content = "\n\n".join(
        f"**Title:** {n['title']}\n**Description:** {n['description']}\n**URL:** {n['url']}"
        for n in all_news
    )

    # 3. Website Scraping
    logging.info("🌐 Finding and scraping company website...")
    website = get_company_website(company, API_KEY, CX)
    if not website:
        logging.error(f"❌ Could not find website for {company}.")
        return
    try:
        website_content, _ = scrape_website(website)
        logging.info("✅ Website scraped successfully.")
    except Exception as e:
        logging.error(f"❌ Website scraping error: {e}")
        return

    # 4. LinkedIn Scraping
    logging.info("🔗 Finding and scraping LinkedIn profile...")
    linkedin_url = get_company_website(company + " LinkedIn", API_KEY, CX)
    if not linkedin_url:
        logging.error(f"❌ Could not find LinkedIn profile for {company}.")
        return
    try:
        linkedin_content, _ = scrape_website(linkedin_url)
        logging.info("✅ LinkedIn scraped successfully.")
    except Exception as e:
        logging.error(f"❌ LinkedIn scraping error: {e}")
        return

    # 5. AI Lead Check
    logging.info("🤖 Performing AI-based lead analysis...")
    try:
        lead_analysis, potential_lead_check = check_potential_lead(
            website_content, linkedin_content, news_content
        )
        logging.info("✅ Lead analysis complete.")
    except Exception as e:
        logging.error(f"❌ OpenAI analysis error: {e}")
        return
    logging.info(f"🔍 Is '{company}' a potential lead? → {potential_lead_check}")

    if potential_lead_check.strip().lower() == "yes":
        logging.info("📌 Lead confirmed. Extracting lead details...")
        try:
            details = extract_lead_details(lead_analysis, company)
            logging.info("✅ Lead details extracted.")
        except Exception as e:
            logging.error(f"❌ Failed to extract lead details: {e}")
            return

        m = re.search(r"\*\*Lead Identification Area\*\*: (.+)", details)
        lead_areas = m.group(1).strip() if m else "Not available"

        try:
            email_flag = add_lead_to_excel(company, lead_areas)
            logging.info("✅ Excel updated successfully.")
        except Exception as e:
            logging.error(f"❌ Excel update error: {e}")
            email_flag = False

        if email_flag:
            try:
                lead_doc_name, lead_doc_stream = create_lead_docx(lead_analysis, company)
                full_doc_name, full_doc_stream = create_full_docx(
                    website_content, linkedin_content, news_content, company
                )
                logging.info("📄 DOCX files generated.")
            except Exception as e:
                logging.error(f"❌ Error generating DOCX files: {e}")
                return

            try:
                token = get_access_token()
                logging.info("🔐 Access token acquired.")
                email_body = (
                    f"<html><body><p>A potential lead has been identified for <strong>{company}</strong>.</p>"
                    + "".join(f"<p>{markdown_bold_to_html(line)}</p>" for line in details.splitlines())
                    + "<p>See attachments for full reports.</p></body></html>"
                )
                sent = send_email(
                    token,
                    ["vishnu.kg@sonata-software.com"],
                    f"Potential Lead Identified – {company}",
                    email_body,
                    attachments=[(lead_doc_name, lead_doc_stream), (full_doc_name, full_doc_stream)]
                )
                if sent:
                    logging.info("✅ Email sent with attachments.")
                    send_lead_data_to_api(lead_areas, my_account_name, my_lead_name, lead_doc_name,  lead_doc_stream)
                    logging.info("📨 Lead data posted to external API.")
                else:
                    logging.warning("⚠️ Email not sent.")
            except Exception as e:
                logging.error(f"❌ Error in email or token process: {e}")
        else:
            logging.info("📝 No new lead areas identified; skipping email.")
    else:
        logging.info(f"🚫 '{company}' is not identified as a potential lead; skipping all downstream steps.")

    logging.info("✅ Lead generation cycle completed.")


# @app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False)
# def timer_trigger(myTimer: func.TimerRequest) -> None:
#     # Your main logic, exactly as you wrote it
#     utc_timestamp = datetime.utcnow()
    
#     if myTimer.past_due:
#         logging.info('The timer is past due!')
    
#     logging.info('Python timer trigger function executed at %s', utc_timestamp)

    
#     # 1) Fixed inputs
#     company = "Computacenter"
#     pages = 1
#     my_account_name = "Computacenter India" 
#     my_lead_name = "Lead from Lead Generator Tool"

#     # 2) Fetch GNews API articles
#     today = datetime.utcnow()
#     frm = (today - timedelta(days=30)).strftime("%Y-%m-%d")
#     to = today.strftime("%Y-%m-%d")
#     news_results = []
#     try:
#         resp = requests.get(
#             "https://gnews.io/api/v4/search",
#             params={"q": company, "from": frm, "to": to, "lang": "en", "token": os.getenv("GNEWS_API_KEY")},
#             verify=True
#         )
#         resp.raise_for_status()
#         for art in resp.json().get("articles", []):
#             news_results.append({
#                 "title": art.get("title", ""),
#                 "description": art.get("description", ""),
#                 "url": art.get("url", "")
#             })
#         logging.info(f"GNews API returned {len(news_results)} articles.")
#     except Exception as e:
#         logging.error(f"GNews API error: {e}")

#     # 3) Scrape Google News pages
#     google_news_results = []
#     try:
#         google_news_results = scrape_google_news(company, pages)
#         logging.info(f"Google News scrape found {len(google_news_results)} articles.")
#     except Exception as e:
#         logging.error(f"Google News scraping error: {e}")

#     all_news = news_results + google_news_results
#     if not all_news:
#         logging.warning("⚠️ No news results found.")
#         return

#     # Build news content for analysis
#     news_content = "\n\n".join(
#         f"**Title:** {n['title']}\n**Description:** {n['description']}\n**URL:** {n['url']}"
#         for n in all_news
#     )

#     # 4) Scrape company website
#     website = get_company_website(company, os.getenv("API_KEY"), os.getenv("CX"))
#     if not website:
#         logging.error(f"Could not find website for {company}.")
#         return
#     try:
#         website_content, _ = scrape_website(website)
#     except Exception as e:
#         logging.error(f"Website scraping error: {e}")
#         return

#     # 5) Scrape LinkedIn profile
#     linkedin_url = get_company_website(company + " LinkedIn", os.getenv("API_KEY"), os.getenv("CX"))
#     if not linkedin_url:
#         logging.error(f"Could not find LinkedIn profile for {company}.")
#         return
#     try:
#         linkedin_content, _ = scrape_website(linkedin_url)
#     except Exception as e:
#         logging.error(f"LinkedIn scraping error: {e}")
#         return

#     # 6) Lead analysis & classification via OpenAI
#     try:
#         lead_analysis, potential_lead_check = check_potential_lead(
#             website_content, linkedin_content, news_content
#         )
#         logging.info("Lead analysis complete.")
#     except Exception as e:
#         logging.error(f"OpenAI analysis error: {e}")
#         return
#     logging.info(f"Is '{company}' a potential lead? {potential_lead_check}")

#     # 7) If Yes, update Excel & send email
#     if potential_lead_check.strip().lower() == "yes":
#         # Extract lead identification area
#         details = extract_lead_details(lead_analysis, company)
#         m = re.search(r"\*\*Lead Identification Area\*\*: (.+)", details)
#         lead_areas = m.group(1).strip() if m else "Not available"

#         try:
#             email_flag = add_lead_to_excel(company, lead_areas)
#         except Exception as e:
#             logging.error(f"Excel update error: {e}")
#             email_flag = False

#         if email_flag:
#             # Create DOCX attachments as in-memory streams
#             lead_doc_name, lead_doc_stream = create_lead_docx(lead_analysis, company)
#             full_doc_name, full_doc_stream = create_full_docx(
#                 website_content, linkedin_content, news_content, company
#             )

#             # Build HTML email body
#             body_lines = details.splitlines()
#             formatted_html = "".join(
#                 f"<p>{markdown_bold_to_html(line)}</p>" for line in body_lines
#             )
#             email_body = (
#                 f"<html><body>"
#                 f"<p>A potential lead has been identified for <strong>{company}</strong>.</p>"
#                 f"<p>Details:</p>{formatted_html}"
#                 f"<p>See attachments for full reports.</p>"
#                 f"</body></html>"
#             )

#             try:
#                 token = get_access_token()
#                 sent = send_email(
#                     token,
#                     ["vishnu.kg@sonata-software.com"],
#                     f"Potential Lead Identified – {company}",
#                     email_body,
#                     # Pass the in-memory streams to the send_email function
#                     attachments=[(lead_doc_name, lead_doc_stream), (full_doc_name, full_doc_stream)]
#                 )
#                 logging.info("✅ Email sent with attachments." if sent else "❌ Email send failed.")
#             except Exception as e:
#                 logging.error(f"Email send error: {e}")
#             # --- NEW STEP: CALL THE API FUNCTION ---
#             if sent:
#                 send_lead_data_to_api(lead_areas, my_account_name, my_lead_name)
#                 logging.info("Lead data successfully sent to external API.")
#         else:
#             logging.info(f"No new lead areas for '{company}'; no email sent.")
#     else:
#         logging.info(f"'{company}' is not identified as a potential lead; skipping email.")


#     logging.info("Lead generation run completed.")










