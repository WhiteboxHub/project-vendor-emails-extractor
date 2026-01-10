import re
import phonenumbers
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class RegexExtractor:
    """Extract contact information using regex patterns"""
    
    def __init__(self, email_filter=None):
        self.email_pattern = re.compile(
            r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+',
            re.IGNORECASE
        )
        
        self.linkedin_pattern = re.compile(
            r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([a-zA-Z0-9\-_]+)',
            re.IGNORECASE
        )
        
        self.logger = logging.getLogger(__name__)
        self.email_filter = email_filter  # DB-driven filter
        
        # Load file extensions from CSV (NO hardcoding)
        self.file_extensions = self._load_file_extensions()
    
    def _load_file_extensions(self) -> set:
        """Load invalid email file extensions from CSV"""
        if not self.email_filter or not hasattr(self.email_filter, 'sender_rules'):
            return set()
        
        extensions = set()
        for rule in self.email_filter.sender_rules:
            if rule['category'] == 'invalid_email_extension':
                for kw in rule['keywords']:
                    if isinstance(kw, str):
                        extensions.add(kw.lower())
        
        if extensions:
            self.logger.info(f"Loaded {len(extensions)} file extensions from CSV")
        return extensions
    
    def _is_personal_email(self, email: str) -> bool:
        """
        DEPRECATED: Use email_filter.is_email_allowed() instead which uses DB rules.
        Kept for backward compatibility - now delegates to DB filter.
        """
        if not email or '@' not in email:
            return True
        
        # Use DB filter if available
        if self.email_filter:
            return not self.email_filter.is_email_allowed(email)
        
        # Fallback: basic check (should not be used if DB filter is available)
        return False
    
    def _is_valid_email_format(self, email: str) -> bool:
        """Validate email format and filter out CID references and fake emails
        
        Filters out:
        - Image CIDs: image001.png@01dc6e1f.089ef930
        - File references: document.pdf@server.com
        - Invalid formats with numbers/hex in domain
        """
        if not email or '@' not in email:
            return False
        
        try:
            local_part, domain = email.split('@', 1)
            
            # Use CSV-loaded file extensions (NO hardcoding)
            if self.file_extensions:
                for ext in self.file_extensions:
                    if local_part.lower().endswith(ext):
                        self.logger.debug(f"Filtered out file/CID: {email}")
                        return False
            
            # Filter out hex-like domains (CID references like @01dc6e1f.089ef930)
            # These typically have only numbers and hex characters
            domain_parts = domain.split('.')
            if all(all(c in '0123456789abcdef' for c in part.lower()) for part in domain_parts):
                self.logger.debug(f"Filtered out CID reference: {email}")
                return False
            
            # Domain should have at least one alphabetic character
            if not any(c.isalpha() for c in domain):
                self.logger.debug(f"Filtered out invalid domain: {email}")
                return False
            
            # Domain should not be too short (minimum realistic: x.co = 4 chars)
            if len(domain) < 4:
                return False
            
            return True
            
        except:
            return False
    
    def extract_email(self, text: str) -> Optional[str]:
        """Extract email address from text using DB filters"""
        try:
            emails = self.email_pattern.findall(text)
            if not emails:
                return None
            
            valid_emails = []
            for email in emails:
                email_lower = email.lower()
                
                # Validate format
                if not self._is_valid_email_format(email_lower):
                    continue
                
                # Use DB filter if available
                if self.email_filter:
                    if not self.email_filter.is_email_allowed(email_lower):
                        self.logger.debug(f"Skipped email (DB filter): {email_lower}")
                        continue
                else:
                    # Fallback: basic check
                    if '@' not in email_lower or '.' not in email_lower.split('@')[1]:
                        continue
                
                valid_emails.append(email_lower)
            
            if valid_emails:
                self.logger.debug(f"Extracted email: {valid_emails[0]}")
                return valid_emails[0]
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting email: {str(e)}")
            return None
    
    def extract_all_emails(self, text: str) -> list:
        """Extract all email addresses from text"""
        try:
            return [email.lower() for email in self.email_pattern.findall(text)]
        except Exception as e:
            self.logger.error(f"Error extracting emails: {str(e)}")
            return []
    
    def extract_phone(self, text: str, region: str = 'US') -> Optional[str]:
        """Extract and format phone number with fallback regions"""
        try:
            # Try US first
            for match in phonenumbers.PhoneNumberMatcher(text, region):
                phone_number = match.number
                # Validate it's a reasonable phone number
                if phonenumbers.is_valid_number(phone_number):
                    return phonenumbers.format_number(
                        phone_number, 
                        phonenumbers.PhoneNumberFormat.E164
                    )
            
            # Fallback: Try without region
            for match in phonenumbers.PhoneNumberMatcher(text, None):
                phone_number = match.number
                if phonenumbers.is_valid_number(phone_number):
                    return phonenumbers.format_number(
                        phone_number, 
                        phonenumbers.PhoneNumberFormat.E164
                    )
                    
        except Exception as e:
            self.logger.error(f"Error extracting phone: {str(e)}")
        return None
    
    def extract_linkedin_id(self, text: str) -> Optional[str]:
        """Extract LinkedIn profile ID from URL"""
        try:
            match = self.linkedin_pattern.search(text)
            return match.group(1) if match else None
        except Exception as e:
            self.logger.error(f"Error extracting LinkedIn: {str(e)}")
            return None
    
    def extract_linkedin_url(self, text: str) -> Optional[str]:
        """Extract full LinkedIn URL"""
        try:
            match = self.linkedin_pattern.search(text)
            return match.group(0) if match else None
        except Exception as e:
            self.logger.error(f"Error extracting LinkedIn URL: {str(e)}")
            return None
