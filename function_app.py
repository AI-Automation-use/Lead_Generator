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

# Load environment variables from the .env file
# NOTE: In Azure Functions, these are loaded from "Application Settings," so this
# line is mainly for local development.
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
# AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# LEAD_EXCEL_CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME")
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=leadgeneratoruat;AccountKey=RiP+k6mn9kflF1gdXPeUjQtisybeEJtEOfNj97+i/ml2imwl8vg74s7x1luYYAMZCYqjpQ/9OkVe+AStSViuWg==;EndpointSuffix=core.windows.net"
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

# --- Azure Blob Storage Functions ---
# ... (all your existing helper functions like get_blob_service_client, 
# download_excel_from_blob, etc., go here)

def get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def download_excel_from_blob(blob_service_client, container_name, blob_name):
    # ... (your existing code)
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        return io.BytesIO(download_stream.readall())
    except Exception as e:
        print(f"Error downloading blob {blob_name}: {e}")
        return None

# def upload_excel_to_blob(blob_service_client, container_name, blob_name, data):
#     # ... (your existing code)
#     try:
#         container_client = blob_service_client.get_container_client(container_name)
#         blob_client = container_client.get_blob_client(blob_name)
#         blob_client.upload_blob(data, overwrite=True)
#         print(f"Successfully uploaded {blob_name} to blob storage.")
#     except Exception as e:
#         print(f"Error uploading blob {blob_name}: {e}")

def get_identified_leads_df():
    # ... (your existing code)
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
    # ... (your existing code)
    if not isinstance(areas_str, str):
        return ""
    parts = areas_str.replace(';', ',').split(',')
    cleaned_parts = sorted(list(set(area.strip() for area in parts if area.strip())))
    return ", ".join(cleaned_parts)

def add_lead_to_excel(company_name, lead_areas):
    # ... (your existing code)
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
    # ... (your existing code)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        content = page.locator("body").inner_text()
        clean_text = content.strip().replace('\n', ' ').replace('\r', ' ')
        return clean_text[:10000] if clean_text else "⚠️ Full article not available."
    except Exception as e:
        print(f"Error fetching full article from {url}: {e}")
        return "⚠️ Full article not available."

def scrape_google_news(company_name, pages=3):
    # ... (your existing code)
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

# def create_lead_docx(lead_analysis: str, company: str) -> str:
#     # ... (your existing code)
#     doc = Document()
#     doc.add_heading(f"Lead Analysis for {company}", 0)
#     doc.add_paragraph(lead_analysis)
#     filename = f"{company}_lead_analysis.docx"
#     doc.save(filename)
#     return filename

# def create_full_docx(website_content: str, linkedin_content: str, news_content: str, company: str) -> str:
#     # ... (your existing code)
#     doc = Document()
#     doc.add_heading("Company Content and Analysis", 0)
#     doc.add_heading("Company Website Content:", level=1)
#     doc.add_paragraph(website_content)
#     doc.add_heading("Company LinkedIn Profile Content:", level=1)
#     doc.add_paragraph(linkedin_content)
#     doc.add_heading("Recent News Articles:", level=1)
#     doc.add_paragraph(news_content)
#     filename = f"{company}_full_content.docx"
#     doc.save(filename)
#     return filename


# Create a temporary BytesIO stream to save the docx content to memory
def create_lead_docx(lead_analysis: str, company: str):
    doc = Document()
    doc.add_heading(f"Lead Analysis for {company}", 0)
    doc.add_paragraph(lead_analysis)
    
    # Save the document to an in-memory stream
    doc_stream = io.BytesIO()
    doc.save(doc_stream)
    doc_stream.seek(0)
    
    filename = f"{company}_lead_analysis.docx"
    return filename, doc_stream

# Create a temporary BytesIO stream to save the docx content to memory
def create_full_docx(website_content: str, linkedin_content: str, news_content: str, company: str):
    doc = Document()
    doc.add_heading("Company Content and Analysis", 0)
    doc.add_heading("Company Website Content:", level=1)
    doc.add_paragraph(website_content)
    doc.add_heading("Company LinkedIn Profile Content:", level=1)
    doc.add_paragraph(linkedin_content)
    doc.add_heading("Recent News Articles:", level=1)
    doc.add_paragraph(news_content)
    
    # Save the document to an in-memory stream
    doc_stream = io.BytesIO()
    doc.save(doc_stream)
    doc_stream.seek(0)
    
    filename = f"{company}_full_content.docx"
    return filename, doc_stream

# New function to upload an in-memory stream to Azure Blob Storage
def upload_excel_to_blob(blob_service_client, container_name, blob_name, data_stream):
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data_stream, overwrite=True)
        print(f"Successfully uploaded {blob_name} to blob storage.")
    except Exception as e:
        print(f"Error uploading blob {blob_name}: {e}")

from azure.identity import DefaultAzureCredential

# New get_access_token function for Managed Identity
def get_access_token():
    logging.info("🔐 Acquiring access token using Managed Identity...")
    try:
        # DefaultAzureCredential automatically handles Managed Identity
        credential = DefaultAzureCredential()
        # The scope for Microsoft Graph
        scope = "https://graph.microsoft.com/.default"
        token = credential.get_token(scope)
        logging.info("✅ Successfully acquired token via Managed Identity.")
        return token.token
    except Exception as e:
        logging.error(f"❌ Failed to acquire token with Managed Identity: {e}")
        raise

# def get_access_token():
#     # ... (your existing code)
#     print("🔐 Acquiring access token...")
#     token_cache = msal.SerializableTokenCache()
#     if os.path.exists(TOKEN_FILE):
#         try:
#             with open(TOKEN_FILE, "r") as f:
#                 token_cache.deserialize(f.read())
#             print("✅ Loaded token from local cache.")
#         except Exception as e:
#             print(f"⚠️ Failed to load token cache: {e}")
#     app = msal.PublicClientApplication(
#         client_id=CLIENT_ID,
#         authority=f"https://login.microsoftonline.com/{TENANT_ID}",
#         token_cache=token_cache
#     )
#     accounts = app.get_accounts()
#     result = app.acquire_token_silent(SCOPES, account=accounts[0]) if accounts else None
#     if not result:
#         flow = app.initiate_device_flow(scopes=SCOPES)
#         if "message" in flow:
#             print(flow["message"])
#         else:
#             raise Exception("❌ Failed to initiate device flow.")
#         result = app.acquire_token_by_device_flow(flow)
#     if token_cache.has_state_changed:
#         with open(TOKEN_FILE, "w") as f:
#             f.write(token_cache.serialize())
#         print("💾 Token cache updated and saved locally.")
#     if "access_token" not in result:
#         raise Exception(f"❌ Token acquisition failed: {result.get('error_description')}")
#     return result["access_token"]

def get_company_website(company_name, api_key, cx):
    # ... (your existing code)
    search_url = f"https://www.googleapis.com/customsearch/v1?q={company_name}+company+site&key={api_key}&cx={cx}"
    response = requests.get(search_url)
    if response.status_code == 200:
        results = response.json()
        if 'items' in results:
            return results['items'][0]['link']
    return None

def scrape_website(website):
    # ... (your existing code)
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
    # ... (your existing code)
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
    # ... (your existing code)
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
    # ... (your existing code)
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

def send_email(access_token, recipient_emails, subject, body, attachment_paths=None):
    # ... (your existing code)
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
    if attachment_paths:
        for path in attachment_paths:
            with open(path, "rb") as f:
                data = f.read()
            encoded = base64.b64encode(data).decode("utf-8")
            message["message"]["attachments"].append({
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": os.path.basename(path),
                "contentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "contentBytes": encoded
            })
    resp = requests.post(
        f"{GRAPH_API_ENDPOINT}/me/sendMail",
        headers=headers,
        json=message
    )
    if resp.status_code == 202:
        print("✅ Email with attachments sent!")
        return True
    else:
        print("❌ Failed to send:", resp.status_code, resp.text)
        return False

def markdown_bold_to_html(text):
    # ... (your existing code)
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

# The Function App and Timer Trigger decorator
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False)
def Computacenter(myTimer: func.TimerRequest) -> None:
    # Your main logic, exactly as you wrote it
    utc_timestamp = datetime.utcnow()
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Python timer trigger function executed at %s', utc_timestamp)

    # 1) Fixed inputs
    company = "Computacenter"
    pages = 1

    # 2) Fetch GNews API articles
    today = datetime.utcnow()
    frm = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to = today.strftime("%Y-%m-%d")
    news_results = []
    try:
        resp = requests.get(
            "https://gnews.io/api/v4/search",
            params={"q": company, "from": frm, "to": to, "lang": "en", "token": os.getenv("GNEWS_API_KEY")},
            verify=True
        )
        resp.raise_for_status()
        for art in resp.json().get("articles", []):
            news_results.append({
                "title": art.get("title", ""),
                "description": art.get("description", ""),
                "url": art.get("url", "")
            })
        logging.info(f"GNews API returned {len(news_results)} articles.")
    except Exception as e:
        logging.error(f"GNews API error: {e}")

    # 3) Scrape Google News pages
    google_news_results = []
    try:
        google_news_results = scrape_google_news(company, pages)
        logging.info(f"Google News scrape found {len(google_news_results)} articles.")
    except Exception as e:
        logging.error(f"Google News scraping error: {e}")

    all_news = news_results + google_news_results
    if not all_news:
        logging.warning("⚠️ No news results found.")
        return

    # Build news content for analysis
    news_content = "\n\n".join(
        f"**Title:** {n['title']}\n**Description:** {n['description']}\n**URL:** {n['url']}"
        for n in all_news
    )

    # 4) Scrape company website
    website = get_company_website(company, os.getenv("API_KEY"), os.getenv("CX"))
    if not website:
        logging.error(f"Could not find website for {company}.")
        return
    try:
        website_content, _ = scrape_website(website)
    except Exception as e:
        logging.error(f"Website scraping error: {e}")
        return

    # 5) Scrape LinkedIn profile
    linkedin_url = get_company_website(company + " LinkedIn", os.getenv("API_KEY"), os.getenv("CX"))
    if not linkedin_url:
        logging.error(f"Could not find LinkedIn profile for {company}.")
        return
    try:
        linkedin_content, _ = scrape_website(linkedin_url)
    except Exception as e:
        logging.error(f"LinkedIn scraping error: {e}")
        return

    # 6) Lead analysis & classification via OpenAI
    try:
        lead_analysis, potential_lead_check = check_potential_lead(
            website_content, linkedin_content, news_content
        )
        logging.info("Lead analysis complete.")
    except Exception as e:
        logging.error(f"OpenAI analysis error: {e}")
        return
    logging.info(f"Is '{company}' a potential lead? {potential_lead_check}")

    # 7) If Yes, update Excel & send email
    if potential_lead_check.strip().lower() == "yes":
        # Extract lead identification area
        details = extract_lead_details(lead_analysis, company)
        m = re.search(r"\*\*Lead Identification Area\*\*: (.+)", details)
        lead_areas = m.group(1).strip() if m else "Not available"

        try:
            email_flag = add_lead_to_excel(company, lead_areas)
        except Exception as e:
            logging.error(f"Excel update error: {e}")
            email_flag = False

        if email_flag:
            # Create DOCX attachments
            lead_doc = create_lead_docx(lead_analysis, company)
            full_doc = create_full_docx(
                website_content, linkedin_content, news_content, company
            )

            # Build HTML email body
            body_lines = details.splitlines()
            formatted_html = "".join(
                f"<p>{markdown_bold_to_html(line)}</p>" for line in body_lines
            )
            email_body = (
                f"<html><body>"
                f"<p>A potential lead has been identified for <strong>{company}</strong>.</p>"
                f"<p>Details:</p>{formatted_html}"
                f"<p>See attachments for full reports.</p>"
                f"</body></html>"
            )

            try:
                token = get_access_token()
                sent = send_email(
                    token,
                    ["vishnu.kg@sonata-software.com"],
                    f"Potential Lead Identified – {company}",
                    email_body,
                    [lead_doc, full_doc]
                )
                logging.info("✅ Email sent with attachments." if sent else "❌ Email send failed.")
            except Exception as e:
                logging.error(f"Email send error: {e}")
        else:
            logging.info(f"No new lead areas for '{company}'; no email sent.")
    else:
        logging.info(f"'{company}' is not identified as a potential lead; skipping email.")


    logging.info("Lead generation run completed.")



