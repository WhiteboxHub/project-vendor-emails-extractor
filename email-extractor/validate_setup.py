# test_my account
#!/usr/bin/env python3
"""
STANDALONE TEST - Extract Vendors from YOUR Email Account
No database required - saves to JSON file
"""

print("start")

import sys
import json
import csv
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logging.logger import get_logger
from utils.email.connectors import GmailIMAPConnector
from utils.email.reader import EmailReader
from utils.preprocessor.cleaner import EmailCleaner
from utils.extraction.extractor import ContactExtractor
from utils.filters.email_filters import EmailFilter
from utils.config import get_config
from utils.uid_tracker import get_uid_tracker

class TestEmailExtractor:
    """Test extractor using your personal email account"""
    
    def __init__(self):
        # Load environment variables
        load_dotenv('.env')
        
        # Get credentials from .env
        self.test_email = os.getenv('TEST_EMAIL')
        self.test_app_password = os.getenv('TEST_APP_PASSWORD')
        self.batch_size = int(os.getenv('TEST_BATCH_SIZE', '100'))  # Reduced default from 10000
        
        if not self.test_email or not self.test_app_password:
            print("[ERROR] ERROR: TEST_EMAIL and TEST_APP_PASSWORD must be set in config/.env")
            print("\nAdd these lines to config/.env:")
            print("  TEST_EMAIL=your.email@gmail.com")
            print("  TEST_APP_PASSWORD=your_16_char_app_password")
            sys.exit(1)
        
        # Setup logging
        self.logger = get_logger(__name__)
        self.logger.info("=" * 80)
        self.logger.info("TEST MODE - Extracting from YOUR account")
        self.logger.info("=" * 80)
        
        # Load config for extractors
        try:
            config_loader = get_config()
            self.config = config_loader.load()
        except Exception as e:
            # Use minimal config if config.yaml fails
            self.config = {
                'email': {
                    'imap_server': 'imap.gmail.com',
                    'imap_port': 993,
                    'batch_size': self.batch_size
                },
                'extraction': {
                    'enabled_methods': ['regex', 'spacy', 'gliner']
                },
                'filters': {
                    # All filters are now managed dynamically via keywords.csv/database
                    # No hardcoded blacklists needed
                },
                'processing': {
                    'calendar_invites': {
                        'process': True
                    }
                }
            }
        
        # Initialize components
        self.cleaner = EmailCleaner()
        self.extractor = ContactExtractor(self.config)
        self.email_filter = EmailFilter(self.config)
        
        # Initialize UID tracker for test mode
        self.uid_tracker = get_uid_tracker('last_run_test.json')
        
        self.logger.info(f"Testing with: {self.test_email}")
        self.logger.info(f"Batch size: {self.batch_size} emails")
    
    def run_test(self):
        """Run extraction test on your account"""
        try:
            print("\n" + "=" * 80)
            print(f"[SEARCH] CONNECTING TO: {self.test_email}")
            print("=" * 80)
            
            # Connect to email (server/port are hardcoded in GmailIMAPConnector)
            connector = GmailIMAPConnector(
                email=self.test_email,
                password=self.test_app_password
            )
            
            if not connector.connect():
                print(f"[ERROR] Failed to connect to {self.test_email}")
                print("\n🔧 Troubleshooting:")
                print("  1. Enable 2FA: https://myaccount.google.com/security")
                print("  2. Create App Password: https://myaccount.google.com/apppasswords")
                print("  3. Use 16-character app password (not regular password)")
                print("  4. Enable IMAP in Gmail Settings")
                return
            
            print("[OK] Connected successfully!")
            
            try:
                reader = EmailReader(connector)
                
                # Check last UID
                last_uid = self.uid_tracker.get_last_uid(self.test_email)
                if last_uid:
                    print(f"\n[INFO] Resuming from last UID: {last_uid}")
                else:
                    print("\n[EMAIL] First run - processing all emails")
                
                # Fetch emails
                print(f"\n[EMAIL] Fetching up to {self.batch_size} emails...")
                emails, _ = reader.fetch_emails(
                    since_uid=last_uid,
                    batch_size=self.batch_size,
                    start_index=0
                )
                
                if not emails:
                    print("[WARNING]  No emails found in inbox")
                    return
                
                print(f"[OK] Fetched {len(emails)} emails")
                
                # Filter emails
                print("\n[SEARCH] Filtering recruiter emails...")
                filter_result = self.email_filter.filter_emails(emails, self.cleaner)
                
                # Ensure we properly unpack the tuple
                if isinstance(filter_result, tuple) and len(filter_result) == 2:
                    filtered_emails, filter_stats = filter_result
                else:
                    print(f"[ERROR] filter_emails returned unexpected type: {type(filter_result)}")
                    self.logger.error(f"filter_emails returned: {type(filter_result)} - {filter_result}")
                    return
                
                # Ensure filtered_emails is a list
                if not isinstance(filtered_emails, list):
                    print(f"[ERROR] filtered_emails is not a list (type: {type(filtered_emails).__name__})")
                    self.logger.error(f"filtered_emails is not a list: {type(filtered_emails)}")
                    return
                
                print(f"[OK] Found {len(filtered_emails)} potential recruiter emails")
                
                # Debug: Check structure of filtered_emails
                if filtered_emails:
                    first_item = filtered_emails[0]
                    print(f"[DEBUG] First filtered email type: {type(first_item).__name__}")
                    if isinstance(first_item, dict):
                        print(f"[DEBUG] First filtered email keys: {list(first_item.keys())}")
                    else:
                        print(f"[DEBUG] First filtered email value: {first_item}")
                
                if not filtered_emails:
                    print("\n[WARNING]  No recruiter emails found after filtering")
                    print("   (All emails were from blacklisted domains or spam)")
                    return
                
                # Extract contacts
                print("\n[TARGET] Extracting vendor contacts...")
                print("="*80)
                contacts = []
                seen_emails = set()
                seen_linkedin = set()
                
                for i, email_data in enumerate(filtered_emails, 1):
                    try:
                        # Ensure email_data is a dictionary
                        if not isinstance(email_data, dict):
                            print(f"\n  [{i}/{len(filtered_emails)}] ✗ Error: email_data is not a dict (type: {type(email_data).__name__})")
                            self.logger.error(f"email_data is not a dict: {type(email_data)} - {email_data}")
                            continue
                        
                        clean_body = email_data.get('clean_body', 
                                                    self.cleaner.extract_body(email_data['message']))
                        
                        # Get subject for better job position extraction
                        subject = email_data.get('subject', '')
                        
                        contacts_list = self.extractor.extract_contacts(
                            email_data['message'],
                            clean_body,
                            source_email=self.test_email,
                            subject=subject
                        )
                        
                        # Process each contact in the list
                        for contact in contacts_list:
                        
                            # Add metadata
                            contact['raw_body'] = clean_body
                            contact['extracted_from_subject'] = email_data.get('subject', 'N/A')
                            contact['extracted_from_date'] = email_data.get('date', 'N/A')
                            contact['extracted_from_uid'] = email_data.get('uid', 'N/A')
                        
                            # Only save if we have email or linkedin
                            if contact.get('email') or contact.get('linkedin_id'):
                                # Check for duplicates
                                email_addr = contact.get('email', '').lower()
                                linkedin_id = (contact.get('linkedin_id') or '').lower()
                            
                                is_duplicate = False
                                if email_addr and email_addr in seen_emails:
                                    is_duplicate = True
                                elif linkedin_id and linkedin_id in seen_linkedin:
                                    is_duplicate = True
                            
                                if not is_duplicate:
                                    contacts.append(contact)
                                    if email_addr:
                                        seen_emails.add(email_addr)
                                    if linkedin_id:
                                        seen_linkedin.add(linkedin_id)
                                
                                    # Enhanced display
                                    vendor_name = contact.get('name', 'N/A')
                                    vendor_company = contact.get('company', 'N/A')
                                    vendor_email = contact.get('email', contact.get('linkedin_id', 'N/A'))
                                    vendor_phone = contact.get('phone', 'N/A')
                                    vendor_position = contact.get('job_position', 'N/A')
                                    vendor_location = contact.get('location', 'N/A')
                                    vendor_zip = contact.get('zip_code', 'N/A')
                                    vendor_emp_type = contact.get('employment_type', 'N/A')
                                
                                    extraction_src = contact.get("extraction_source", "unknown")
                                    
                                    print(f"\n  [{i}/{len(filtered_emails)}] ✓ EXTRACTED VENDOR (from: {extraction_src}):")
                                    print(f"      Name:     {vendor_name}")
                                    print(f"      Company:  {vendor_company}")
                                    print(f"      Email:    {vendor_email}")
                                    print(f"      Phone:    {vendor_phone}")
                                    print(f"      Position: {vendor_position}")
                                    print(f"      Emp Type: {vendor_emp_type}")
                                    print(f"      Location: {vendor_location}")
                                    print(f"      ZIP:      {vendor_zip}")
                                    print(f"      Subject:  {email_data.get('subject', 'N/A')[:60]}...")
                                else:
                                    print(f"\n  [{i}/{len(filtered_emails)}] ⊘ Duplicate: {contact.get('email', contact.get('linkedin_id', 'N/A'))}")
                            else:
                                print(f"\n  [{i}/{len(filtered_emails)}] ⊘ Skipped: No email or LinkedIn found")
                            
                    except Exception as e:
                        print(f"\n  [{i}/{len(filtered_emails)}] ✗ Error: {str(e)}")
                        self.logger.error(f"Extraction error for email {i}: {str(e)}", exc_info=True)
                        continue
                
                print("\n" + "="*80)
                
                # Update last UID
                if emails:
                    max_uid = max(int(e['uid']) for e in emails)
                    self.uid_tracker.update_last_uid(self.test_email, str(max_uid))
                    print(f"\n[INFO] Updated last UID to: {max_uid}")
                
                # Save to JSON
                self.save_results(contacts)
                
                # Print summary
                print("\n" + "=" * 80)
                print("[STATS] EXTRACTION SUMMARY")
                print("=" * 80)
                print(f"Total emails fetched:      {len(emails)}")
                print(f"Recruiter emails found:    {len(filtered_emails)}")
                print(f"Contacts extracted:        {len(contacts)}")
                print()
                print("Breakdown by field:")
                names_found = sum(1 for c in contacts if c.get('name'))
                emails_found = sum(1 for c in contacts if c.get('email'))
                phones_found = sum(1 for c in contacts if c.get('phone'))
                companies_found = sum(1 for c in contacts if c.get('company'))
                linkedin_found = sum(1 for c in contacts if c.get('linkedin_id'))
                positions_found = sum(1 for c in contacts if c.get('job_position'))
                locations_found = sum(1 for c in contacts if c.get('location'))
                zips_found = sum(1 for c in contacts if c.get('zip_code'))
                emp_types_found = sum(1 for c in contacts if c.get('employment_type'))
                print(f"  - Names:      {names_found}/{len(contacts)} ({names_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Emails:     {emails_found}/{len(contacts)} ({emails_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Phones:     {phones_found}/{len(contacts)} ({phones_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Companies:  {companies_found}/{len(contacts)} ({companies_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - LinkedIn:   {linkedin_found}/{len(contacts)} ({linkedin_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Positions:  {positions_found}/{len(contacts)} ({positions_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Emp Types:  {emp_types_found}/{len(contacts)} ({emp_types_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - Locations:  {locations_found}/{len(contacts)} ({locations_found*100//len(contacts) if contacts else 0}%)")
                print(f"  - ZIP Codes:  {zips_found}/{len(contacts)} ({zips_found*100//len(contacts) if contacts else 0}%)")
                print("=" * 80)
                
            finally:
                connector.disconnect()
                print("\n[OK] Disconnected from email")
                
        except Exception as e:
            self.logger.error(f"Test failed: {str(e)}", exc_info=True)
            print(f"\n[ERROR] Error: {str(e)}")
    
    def save_results(self, contacts):
        """Save contacts to JSON file"""
        if not contacts:
            print("\n[WARNING]  No contacts to save")
            return
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"test_results_{timestamp}.json"
        
        # Prepare data
        output_data = {
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'source_email': self.test_email,
                'total_contacts': len(contacts),
                'batch_size': self.batch_size
            },
            'contacts': contacts
        }
        
        # Save to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n[SAVE] Results saved to: {output_file}")
        
        # Save RAW POSITIONS to CSV for verification
        csv_file = f"raw_positions_test_{timestamp}.csv"
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Header matches raw_position table
                writer.writerow([
                    'candidate_id', 'source', 'source_uid', 'raw_title', 
                    'raw_company', 'raw_location', 'raw_zip', 
                    'raw_description', 'raw_contact_info'
                ])
                
                for contact in contacts:
                    # Construct raw_contact_info JSON
                    contact_info = {
                        "name": contact.get("name"),
                        "email": contact.get("email"),
                        "phone": contact.get("phone"),
                        "linkedin": contact.get("linkedin_id")
                    }
                    
                    writer.writerow([
                        '12345', # Dummy candidate_id
                        'email',
                        contact.get('extracted_from_uid', ''),
                        contact.get('job_position', ''),
                        contact.get('company', ''),
                        contact.get('location', ''),
                        contact.get('zip_code', ''),
                        contact.get('raw_body', '')[:5000], # Limit size for CSV
                        json.dumps(contact_info)
                    ])
            print(f"[SAVE] Raw positions saved to: {csv_file}")
        except Exception as e:
            print(f"[ERROR] Failed to save CSV: {e}")

        # Save RAW POSITIONS to JSON for verification
        json_raw_file = f"raw_positions_test_{timestamp}.json"
        try:
            raw_positions_list = []
            for contact in contacts:
                # Construct raw_contact_info JSON
                contact_info = {
                    "name": contact.get("name"),
                    "email": contact.get("email"),
                    "phone": contact.get("phone"),
                    "linkedin": contact.get("linkedin_id")
                }
                
                raw_payload = {
                    "candidate_id": 12345, # Dummy candidate_id
                    "source": "email",
                    "source_uid": contact.get("extracted_from_uid"),
                    "extractor_version": "v2.0",
                    "raw_title": contact.get("job_position"),
                    "raw_company": contact.get("company"),
                    "raw_location": contact.get("location"),
                    "raw_zip": contact.get("zip_code"),
                    "raw_description": contact.get("raw_body"),
                    "raw_contact_info": json.dumps(contact_info),
                    "raw_notes": f"Extracted from {contact.get('extraction_source')}",
                    "raw_payload": json.dumps(contact),
                    "processing_status": "new"
                }
                raw_positions_list.append(raw_payload)
            
            with open(json_raw_file, 'w', encoding='utf-8') as f:
                json.dump(raw_positions_list, f, indent=2, ensure_ascii=False)
                
            print(f"[SAVE] Raw positions JSON saved to: {json_raw_file}")
        except Exception as e:
            print(f"[ERROR] Failed to save JSON: {e}")

        print(f"   Total contacts: {len(contacts)}")
        
        # Print sample contacts
        print(f"\n[LIST] DETAILED CONTACT RESULTS:")
        print("=" * 80)
        for i, contact in enumerate(contacts, 1):
            print(f"\n{i}. {contact.get('name', 'N/A')}")
            print(f"   Email:        {contact.get('email', 'N/A')}")
            print(f"   Phone:        {contact.get('phone', 'N/A')}")
            print(f"   Company:      {contact.get('company', 'N/A')}")
            print(f"   Position:     {contact.get('job_position', 'N/A')}")
            print(f"   LinkedIn:     {contact.get('linkedin_id', 'N/A')}")
            print(f"   Location:     {contact.get('location', 'N/A')}")
            print(f"   ZIP Code:     {contact.get('zip_code', 'N/A')}")
            print(f"   From Subject: {contact.get('extracted_from_subject', 'N/A')[:50]}...")
            print(f"   Date:         {contact.get('extracted_from_date', 'N/A')}")
            print(f"   UID:          {contact.get('extracted_from_uid', 'N/A')}")
            
            if i == 1:
                print("\n   [SIMULATION] RAW_POSITION PAYLOAD:")
                print("   {")
                print(f"      'candidate_id': <ID_FROM_DB>,")
                print(f"      'source': 'email',")
                print(f"      'source_uid': '{contact.get('extracted_from_uid', 'N/A')}',")
                print(f"      'raw_title': '{contact.get('job_position', '')}',")
                print(f"      'raw_company': '{contact.get('company', '')}',")
                print(f"      'raw_location': '{contact.get('location', '')}',")
                print(f"      'raw_zip': '{contact.get('zip_code', '')}',")
                # Truncate description for display
                desc = contact.get('raw_body', '')[:50] + '...' if contact.get('raw_body') else ''
                print(f"      'raw_description': '{desc}',")
                print(f"      'raw_contact_info': '{{\"name\": \"{contact.get('name')}\", \"email\": \"{contact.get('email')}\", ...}}'")
                print("   }")
        
        print("\n" + "=" * 80)


def main():
    """Main entry point"""
    print("""
================================================================================
    EMAIL VENDOR EXTRACTOR - TEST MODE (IMPROVED)
================================================================================
    
This test will:
  1. Connect to YOUR Gmail account
  2. Fetch recent emails (no database needed)
  3. Extract vendor/recruiter contacts using IMPROVED logic:
     - HTML span tag extraction (<span>Name - Company</span>)
     - Enhanced name/company detection from email headers
     - Better validation and duplicate detection
  4. Save results to JSON file
  
Setup Required:
  1. Add to config/.env:
     TEST_EMAIL=your.email@gmail.com
     TEST_APP_PASSWORD=your_16_char_app_password
     TEST_BATCH_SIZE=50  (optional, default: 50)
  
  2. Gmail App Password:
     - Enable 2FA: https://myaccount.google.com/security
     - Create App Password: https://myaccount.google.com/apppasswords
     - Use 16-character password (not regular password)
  
================================================================================
    """)
    
    try:
        tester = TestEmailExtractor()
        tester.run_test()
        
        print("\n" + "=" * 80)
        print("[OK] TEST COMPLETE - Check results above")
        print("=" * 80)
        print("\nIMPROVEMENTS APPLIED:")
        print("  ✓ HTML span tag extraction for vendor info")
        print("  ✓ Enhanced name extraction from email headers")
        print("  ✓ Better company detection from domains")
        print("  ✓ Improved duplicate filtering")
        print("  ✓ Detailed logging per contact")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n\n[WARNING]  Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()