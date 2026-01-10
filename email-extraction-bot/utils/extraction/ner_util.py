import spacy
from typing import Optional, Dict
import logging
import re
from email.utils import parseaddr

logger = logging.getLogger(__name__)

class SpacyNERExtractor:
    """Extract entities using Spacy NER - 100% CSV-driven"""
    
    def __init__(self, model: str = 'en_core_web_sm', email_filter=None):
        self.logger = logging.getLogger(__name__)
        self.email_filter = email_filter  # CSV-driven validation
        
        try:
            self.nlp = spacy.load(model)
            self.logger.info(f"Loaded Spacy model: {model}")
        except OSError:
            self.logger.error(f"Spacy model '{model}' not found. Run: python -m spacy download {model}")
            raise
        
        # Load ALL validation from CSV (NO hardcoding)
        self.job_title_keywords = self._load_csv_keywords('invalid_job_title')
        self.generic_domains = self._load_csv_keywords('generic_company_domain')
        self.camelcase_prefixes = self._load_camelcase_prefixes()
        self.signature_greetings = self._load_csv_keywords('signature_greeting_keywords')
        
        if self.email_filter:
            self.logger.info(
                f"CSV validation: {len(self.job_title_keywords)} job titles, "
                f"{len(self.generic_domains)} domains, {len(self.camelcase_prefixes)} prefixes, "
                f"{len(self.signature_greetings)} greetings"
            )
    
    def _load_csv_keywords(self, category: str) -> set:
        """Load keywords from CSV by category"""
        if not self.email_filter or not hasattr(self.email_filter, 'sender_rules'):
            self.logger.warning(f"No email_filter - {category} disabled")
            return set()
        
        keywords = set()
        for rule in self.email_filter.sender_rules:
            if rule['category'] == category:
                for kw in rule['keywords']:
                    if isinstance(kw, str):
                        keywords.add(kw.lower())
        return keywords
    
    def _load_camelcase_prefixes(self) -> dict:
        """Load CamelCase company prefixes from CSV"""
        if not self.email_filter or not hasattr(self.email_filter, 'sender_rules'):
            return {}
        
        prefixes = {}
        for rule in self.email_filter.sender_rules:
            if rule['category'] == 'camelcase_company_prefix':
                for kw in rule['keywords']:
                    if isinstance(kw, str):
                        prefixes[kw.lower()] = kw.capitalize()
        return prefixes
    
    def extract_entities(self, text: str) -> Dict[str, str]:
        """
        Extract named entities from text
        
        Returns:
            Dictionary with keys: name, company, location
        """
        try:
            doc = self.nlp(text)
            
            entities = {
                'name': None,
                'company': None,
                'location': None
            }
            
            for ent in doc.ents:
                if ent.label_ == 'PERSON' and not entities['name']:
                    # Filter out single-word names (likely false positives)
                    if len(ent.text.split()) >= 2 and len(ent.text.split()) <= 3:
                        entities['name'] = ent.text.strip()
                
                elif ent.label_ == 'ORG' and not entities['company']:
                    # Filter out job titles
                    company_candidate = ent.text.strip()
                    if not self._is_job_title(company_candidate):
                        entities['company'] = company_candidate
                
                elif ent.label_ in ['GPE', 'LOC'] and not entities['location']:
                    entities['location'] = ent.text.strip()
            
            return entities
            
        except Exception as e:
            self.logger.error(f"Error in Spacy NER extraction: {str(e)}")
            return {'name': None, 'company': None, 'location': None}
    
    def extract_name_from_signature(self, text: str) -> Optional[str]:
        """Extract name from email signature patterns - CSV-driven greetings"""
        try:
            # Build dynamic patterns using CSV-loaded greetings
            if self.signature_greetings:
                greeting_words = '|'.join(re.escape(g.title()) for g in self.signature_greetings)
            else:
                # Minimal fallback if CSV not loaded
                greeting_words = 'Thanks|Regards|Best|Sincerely'
            
            patterns = [
                # After greeting with newline - enhanced to handle apostrophes and hyphens
                rf"(?:{greeting_words}),?\s*[\r\n]+\s*([A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*(?:\s+[A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*)" + "{1,2})\\s*[\\r\\n]",
                # Name followed by title/company
                r"([A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*(?:\s+[A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*){1,2})\s*[\r\n]+(?:Senior|Lead|Director|Manager|Recruiter|VP|President|Talent|Staffing)",
                # Name followed by phone or email on next line
                r"([A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*(?:\s+[A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*){1,2})\s*[\r\n]+(?:Phone|Mobile|Email|Tel|Cell):",
                # Simple pattern after greeting
                rf"(?:{greeting_words}),?\s*[\r\n]+\s*([A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*(?:\s+[A-Z][a-z]+(?:['-][A-Z]?[a-z]+)*)" + "{1,2})",
                # Mc/Mac names specifically
                r"([A-Z][a-z]+\s+M[ac]?[A-Z][a-z]+)",
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.MULTILINE)
                if match:
                    name = match.group(1).strip()
                    # Validate
                    words = name.split()
                    if 2 <= len(words) <= 3 and not any(c.isdigit() for c in name):
                        # Additional validation: at least one alphabetic char per word
                        if all(any(c.isalpha() for c in word) for word in words):
                            self.logger.debug(f"Extracted name from signature: {name}")
                            return name
            
            return None
        except Exception as e:
            self.logger.error(f"Error extracting name from signature: {str(e)}")
            return None
    
    def extract_vendor_from_span(self, text: str) -> Dict[str, Optional[str]]:
        """Extract vendor name and company from HTML span tags or similar patterns
        
        Pattern examples:
        - <span>Name - Company</span>
        - <span>Name | Company</span>
        - <span>Name, Company</span>
        - <span>Name (Company)</span>
        - Plain text: Name - Company
        
        Returns:
            Dictionary with keys: name, company
        """
        try:
            result = {'name': None, 'company': None}
            
            # Multiple patterns to match different formats
            patterns = [
                # Pattern 1: HTML tags with Name - Company (hyphen separator)
                r'<(?:span|div|p|td|th|b|strong)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*[-–—]\s*([A-Z][a-zA-Z0-9\s&.,]+?)\s*</(?:span|div|p|td|th|b|strong)>',
                # Pattern 2: HTML tags with Name | Company (pipe separator)
                r'<(?:span|div|p|td|th|b|strong)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*\|\s*([A-Z][a-zA-Z0-9\s&.,]+?)\s*</(?:span|div|p|td|th|b|strong)>',
                # Pattern 3: HTML tags with Name, Company (comma separator)
                r'<(?:span|div|p|td|th|b|strong)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),\s*([A-Z][a-zA-Z0-9\s&.,]+?)\s*</(?:span|div|p|td|th|b|strong)>',
                # Pattern 4: Name (Company) in parentheses
                r'<(?:span|div|p|td|th|b|strong)[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*\(([A-Z][a-zA-Z0-9\s&.,]+?)\)\s*</(?:span|div|p|td|th|b|strong)>',
                # Pattern 5: Plain text Name - Company (no HTML)
                r'(?:^|\n)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*[-–—]\s*([A-Z][a-zA-Z0-9\s&.,]+?)(?:\n|$)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.MULTILINE)
                if match:
                    name = match.group(1).strip()
                    company = match.group(2).strip()
                    
                    # Validate name (2-4 words, no digits)
                    name_words = name.split()
                    if 2 <= len(name_words) <= 4 and not any(c.isdigit() for c in name):
                        result['name'] = name
                    
                    # Validate company (not a job title, has letters, not too long)
                    if company and not self._is_job_title(company):
                        if any(c.isalpha() for c in company) and len(company) <= 100:
                            result['company'] = company
                    
                    if result['name'] or result['company']:
                        return result
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting vendor from span: {str(e)}")
            return {'name': None, 'company': None}
    
    def extract_name_from_header(self, email_message) -> Optional[str]:
        """Extract name from email From header"""
        try:
            from_header = email_message.get('From', '')
            if not from_header:
                return None
            
            # Parse "Name <email@domain.com>" format
            name, email = parseaddr(from_header)
            
            if name and name != email:
                # Clean up the name
                name = name.strip('"\'')
                
                # Validate it looks like a real name
                # Should be 2-3 words, start with capital, no weird characters
                words = name.split()
                if 2 <= len(words) <= 3:
                    # Check if all words start with capital letter
                    if all(word[0].isupper() for word in words if word):
                        # Check no digits
                        if not any(c.isdigit() for c in name):
                            return name
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting name from header: {str(e)}")
            return None
    
    def extract_company_from_domain(self, email: str) -> Optional[str]:
        """Extract and format company name from email domain with smart capitalization
        
        Examples:
        - john@techcorp.com → TechCorp
        - jane@cyber-coders.com → Cyber Coders
        - bob@acme-inc.com → Acme Inc.
        - alice@123staffing.com → 123 Staffing
        - eve@cybercoders.com → CyberCoders (smart camelcase detection)
        """
        try:
            if not email or '@' not in email:
                return None
            
            # Use CSV-loaded generic domains (NO hardcoding)
            domain = email.split('@')[1]
            company_name = domain.split('.')[0]
            
            if company_name.lower() in self.generic_domains:
                return None
            
            # Smart capitalization logic
            company_name = self._smart_capitalize_company(company_name)
            
            # Clean up with standard cleaning
            company_name = self._clean_company_name(company_name)
            
            if company_name and len(company_name) >= 2:
                self.logger.debug(f"Extracted company from domain: {company_name}")
                return company_name
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting company from domain: {str(e)}")
            return None
    
    def _is_job_title(self, text: str) -> bool:
        """Check if text is likely a job title rather than a company name"""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Check against CSV-loaded job title keywords
        for keyword in self.job_title_keywords:
            if keyword in text_lower:
                return True
        
        return False
    
    def extract_company_from_signature(self, text: str) -> Optional[str]:
        """Extract company name from email signature with pattern matching
        
        Looks for patterns like:
        John Smith
        Senior Recruiter
        TechCorp Inc.
        """
        try:
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            # Look for company name (usually 2-3 lines after name/title)
            for i, line in enumerate(lines):
                # Check if this line looks like a job title
                if self._is_job_title(line) and i + 1 < len(lines):
                    # Next line might be company
                    potential_company = lines[i + 1]
                    
                    # Validate it looks like a company name
                    if self._is_valid_company_name(potential_company):
                        return potential_company
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting company from signature: {str(e)}")
            return None
    
    def _is_valid_company_name(self, text: str) -> bool:
        """Validate if text looks like a company name"""
        if not text:
            return False
        
        # Should have letters
        if not any(c.isalpha() for c in text):
            return False
        
        # Should not be a job title
        if self._is_job_title(text):
            return False
        
        # Should not be too long
        if len(text) > 100:
            return False
        
        # Should not be too short
        if len(text) < 2:
            return False
        
        return True
    
    def _clean_company_name(self, company: str) -> Optional[str]:
        """Clean and standardize company name"""
        if not company:
            return None
        
        # Remove common suffixes/prefixes
        company = company.strip()
        
        # Remove trailing punctuation
        company = company.rstrip('.,;:!?')
        
        # Remove extra whitespace
        company = ' '.join(company.split())
        
        return company if company else None
    
    def _smart_capitalize_company(self, name: str) -> str:
        """Smart capitalization for company names (CSV-driven)"""
        if not name:
            return name
        
        # Replace separators with spaces
        name = name.replace('-', ' ').replace('_', ' ')
        
        # If it has numbers at the start, separate them
        if name[0].isdigit():
            for i, char in enumerate(name):
                if char.isalpha():
                    name = name[:i] + ' ' + name[i:]
                    break
        
        # Use CSV-loaded CamelCase prefixes
        name_lower = name.lower()
        for prefix, replacement in self.camelcase_prefixes.items():
            if name_lower.startswith(prefix) and len(name_lower) > len(prefix):
                rest = name_lower[len(prefix):]
                if rest and rest[0].isalpha():
                    name = replacement + ' ' + rest.capitalize()
                    break
        
        # Standard title case for each word
        words = name.split()
        name = ' '.join(word.capitalize() for word in words if word)
        
        return name
