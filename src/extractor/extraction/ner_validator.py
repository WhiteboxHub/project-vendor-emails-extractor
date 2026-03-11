import logging
import re
from typing import Dict, Optional, Any
from pathlib import Path
import json
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class NERValidator:
    """
    Post-classification validator that uses NER (GLiNER/Regex) to verify 
    and finalize job titles, company names, and URLs.
    """
    
    def __init__(self, use_gliner: bool = False):
        self.logger = logging.getLogger("ner_validator")
        self.ner_log = Path("ner_validation.log")
        self.use_gliner = use_gliner
        self.gliner_extractor = None
        
        # Configure dedicated NER logging
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith("ner_validation.log") for h in self.logger.handlers):
            file_handler = logging.FileHandler("ner_validation.log")
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.INFO)

        if self.use_gliner:
            try:
                from src.extractor.extraction.nlp_gliner import GLiNERExtractor
                # Minimal config for GLiNER
                self.gliner_extractor = GLiNERExtractor({'gliner': {'threshold': 0.5}})
                self.logger.info("GLiNER initialized for NER validation")
            except Exception as e:
                self.logger.error(f"Failed to initialize GLiNER: {e}. Falling back to regex validation.")
                self.use_gliner = False

    def validate_and_finalize(self, raw_job: Dict, job_data: Dict, llm_result: Dict) -> Dict:
        """
        Validates job data and finalizes results for DB insertion.
        Returns updated job_data and validation status.
        """
        raw_id = raw_job.get('id')
        original_title = job_data.get('title')
        original_company = job_data.get('company_name')
        original_url = job_data.get('job_url')
        
        # Deep extraction from raw_payload if available
        payload = raw_job.get('raw_payload') or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except:
                payload = {}
        
        validation_errors = []
        updates = {}
        
        # 1. Validate/Recover Job URL
        url_valid, final_url = self._validate_url(original_url)
        if not url_valid:
            # Try fallback from payload - strictly use job_url only
            fallback_url = payload.get('job_url')
            if fallback_url:
                url_valid, final_url = self._validate_url(fallback_url)
            
            if not url_valid:
                validation_errors.append(f"Invalid or missing job_url")
        updates['job_url'] = final_url

        # 2. Validate/Recover Company Name
        company_valid, final_company = self._validate_company(original_company, raw_job.get('raw_description', ''))
        if not company_valid:
            # Try fallback from payload
            fallback_comp = payload.get('company') or payload.get('company_name') or payload.get('org')
            if fallback_comp:
                company_valid, final_company = self._validate_company(fallback_comp, raw_job.get('raw_description', ''))
            
            if not company_valid:
                validation_errors.append(f"Suspicious Company: {original_company}")
        updates['company_name'] = final_company

        # 3. Validate/Recover Job Title
        title_to_check = llm_result.get('extracted_title') or original_title
        title_valid, final_title = self._validate_title(title_to_check, raw_job.get('raw_description', ''))
        if not title_valid:
            # Try fallback from payload
            fallback_title = payload.get('job_title') or payload.get('title') or payload.get('role')
            if fallback_title:
                title_valid, final_title = self._validate_title(fallback_title, raw_job.get('raw_description', ''))
            
            if not title_valid:
                validation_errors.append(f"Suspicious Title: {title_to_check}")
        updates['title'] = final_title

        # Final Status
        is_finalized = len(validation_errors) == 0
        
        # Log validation results
        log_msg = f"ID: {raw_id:6} | Finalized: {str(is_finalized):5} | Errors: {', '.join(validation_errors) if validation_errors else 'None'}"
        self.logger.info(log_msg)
        
        # Update job_data with finalized values
        job_data.update(updates)
        
        return {
            "is_finalized": is_finalized,
            "errors": validation_errors,
            "job_data": job_data
        }

    def _validate_url(self, url: Optional[str]) -> (bool, str):
        """Checks if URL is valid and not a placeholder."""
        if not url or not isinstance(url, str):
            return False, ""
        
        url = url.strip()
        if not url:
            return False, ""
            
        # Basic regex for URL validation
        url_pattern = re.compile(
            r'^(?:http|ftp)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain
            r'localhost|' # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ip
            r'(?::\d+)?' # port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if not re.match(url_pattern, url):
            return False, url
            
        # Filter out generic domains if they don't have a path
        parsed = urlparse(url)
        generic_domains = ['linkedin.com', 'google.com', 'indeed.com', 'glassdoor.com']
        if parsed.netloc.replace('www.', '') in generic_domains and (not parsed.path or parsed.path == '/'):
            return False, url
            
        return True, url

    def _validate_company(self, company: Optional[str], description: str) -> (bool, str):
        """Cross-references company name."""
        if not company or company.lower() in ['unknown', 'unknown company', 'n/a', 'null']:
            # Try to extract via GLiNER if available
            if self.gliner_extractor:
                extracted = self.gliner_extractor.extract_entities(description)
                new_company = extracted.get('company')
                if new_company:
                    return True, new_company
            return False, company or ""
            
        # Basic sanity check: shouldn't be too long or contain common junk patterns
        if len(company) > 100 or len(company) < 2:
            return False, company
            
        return True, company

    def _validate_title(self, title: Optional[str], description: str) -> (bool, str):
        """Cross-references job title."""
        if not title or title.lower() in ['unknown title', 'untitled position', 'n/a', 'null']:
            return False, title or ""
            
        # Should contain at least one role-related keyword
        role_keywords = ['engineer', 'developer', 'manager', 'architect', 'analyst', 'lead', 'consultant', 'specialist', 'designer']
        if not any(kw in title.lower() for kw in role_keywords):
            # If GLiNER is available, try to fix it
            if self.gliner_extractor:
                extracted = self.gliner_extractor.extract_entities(description)
                new_title = extracted.get('job_title')
                if new_title and any(kw in new_title.lower() for kw in role_keywords):
                    return True, new_title
            return False, title
            
        return True, title
