"""
Job Position Extractor - Extract job titles/positions from recruiter emails

This module provides multiple methods for extracting job positions:
1. Regex-based pattern matching
2. SpaCy noun phrase extraction with trigger words
3. Confidence scoring and validation
"""

import re
import logging
from typing import Optional, List, Dict
import spacy
from utils.filters.filter_repository import get_filter_repository

logger = logging.getLogger(__name__)


class PositionExtractor:
    """Extract job positions from email text using multiple methods"""
    
    def __init__(self, spacy_model=None):
        self.logger = logging.getLogger(__name__)
        
        # Load spaCy model if provided
        self.nlp = spacy_model
        if not self.nlp:
            try:
                self.nlp = spacy.load('en_core_web_sm')
            except:
                self.logger.warning("SpaCy model not loaded - spacy extraction will be disabled")
        
        # Load filter repository for trigger words
        self.filter_repo = get_filter_repository()
        self.job_title_keywords = self._load_job_title_keywords()
        
        # Regex patterns for job position extraction
        self.position_patterns = [
            # "looking for a Senior Java Developer"
            r'(?:looking for|seeking|hiring|need|require|searching for)\s+(?:a\s+|an\s+)?([A-Z][a-zA-Z\s/\-\.]+?(?:Developer|Engineer|Architect|Manager|Analyst|Designer|Consultant|Specialist|Administrator|Coordinator|Lead|Director|Programmer|Tester|Scientist|Researcher))',
            
            # "Position: Senior Java Developer"
            r'(?:position|role|opening|opportunity|job title|title):\s*([A-Z][a-zA-Z\s/\-\.]+)',
            
            # "for the Senior Java Developer position"
            r'for\s+the\s+([A-Z][a-zA-Z\s/\-\.]+?(?:position|role|opening))',
            
            # "Senior Java Developer - Contract"
            r'^([A-Z][a-zA-Z\s/\-\.]+?(?:Developer|Engineer|Architect|Manager|Analyst|Designer|Consultant|Specialist))\s*[-–—]\s*(?:Contract|Full[- ]?time|Part[- ]?time|Permanent|Temporary)',
            
            # "Job: Senior Java Developer"
            r'(?:Job|Vacancy|Req):\s*([A-Z][a-zA-Z\s/\-\.]+)',
            
            # "We have an opening for Senior Java Developer"
            r'opening for\s+(?:a\s+|an\s+)?([A-Z][a-zA-Z\s/\-\.]+)',
            
            # Subject line patterns (often just the job title)
            r'^([A-Z][a-zA-Z\s/\-\.]+?(?:Developer|Engineer|Architect|Manager|Analyst|Designer|Consultant|Specialist|Administrator|Coordinator|Lead|Director|Programmer|Tester|Scientist|Researcher))$',
        ]
        
        # Common job title suffixes for validation
        self.job_title_suffixes = {
            'developer', 'engineer', 'architect', 'manager', 'analyst', 'designer',
            'consultant', 'specialist', 'administrator', 'coordinator', 'lead',
            'director', 'programmer', 'tester', 'scientist', 'researcher', 'officer',
            'executive', 'associate', 'representative', 'agent', 'advisor'
        }
        
        # Common prefixes to remove
        self.prefixes_to_remove = [
            'the ', 'a ', 'an ', 'our ', 'your ', 'this ', 'that ',
            'position of ', 'role of ', 'job of '
        ]
        
        # Marketing/fluff words to remove
        self.marketing_words = [
            'highly skilled', 'highly-skilled', 'innovative', 'experienced',
            'talented', 'exceptional', 'outstanding', 'expert', 'professional',
            'qualified', 'certified', 'proven', 'dedicated', 'motivated',
            'dynamic', 'results-driven', 'results driven', 'top-notch', 'top notch',
            'world-class', 'world class', 'best-in-class', 'best in class'
        ]
        
        # Common artifacts to remove from end of position
        self.trailing_artifacts = [
            'location', 'duration', 'role', 'position', 'opening', 'opportunity',
            'job', 'vacancy', 'req', 'requirement', 'needed', 'wanted'
        ]
    
    def _load_job_title_keywords(self) -> set:
        """Load job title keywords from filter repository (CSV)"""
        try:
            keyword_lists = self.filter_repo.get_keyword_lists()
            
            if 'job_position_trigger_words' in keyword_lists:
                keywords = keyword_lists['job_position_trigger_words']
                job_titles = {kw.lower().strip() for kw in keywords}
                self.logger.info(f"✓ Loaded {len(job_titles)} job position trigger words from CSV")
                return job_titles
            else:
                self.logger.warning("⚠ job_position_trigger_words not found in CSV - using empty set")
                return set()
                
        except Exception as e:
            self.logger.error(f"Failed to load job title keywords from CSV: {str(e)}")
            return set()
    
    def extract_job_position_regex(self, text: str) -> Optional[str]:
        """
        Extract job position using regex patterns
        
        Args:
            text: Email body or subject text
            
        Returns:
            Extracted job position or None
        """
        try:
            if not text:
                return None
            
            # Try each pattern
            for pattern in self.position_patterns:
                matches = re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE)
                
                for match in matches:
                    position = match.group(1).strip()
                    
                    # Clean up the position
                    position = self._clean_position(position)
                    
                    # Validate
                    if self._is_valid_position(position):
                        self.logger.debug(f"✓ Regex extracted position: {position}")
                        return position
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in regex position extraction: {str(e)}")
            return None
    
    def extract_job_position_spacy(self, text: str) -> Optional[str]:
        """
        Extract job position using spaCy noun phrase extraction with trigger words
        
        Args:
            text: Email body text
            
        Returns:
            Extracted job position or None
        """
        try:
            if not self.nlp or not text:
                return None
            
            # Process text with spaCy
            doc = self.nlp(text[:2000])  # Limit to first 2000 chars for performance
            
            candidates = []
            
            # Extract noun phrases that contain job title trigger words
            for chunk in doc.noun_chunks:
                chunk_text = chunk.text.strip()
                chunk_lower = chunk_text.lower()
                
                # Check if chunk contains any trigger word
                has_trigger = any(trigger in chunk_lower for trigger in self.job_title_keywords)
                
                if has_trigger:
                    # Clean and validate
                    position = self._clean_position(chunk_text)
                    
                    if self._is_valid_position(position):
                        # Calculate confidence based on trigger word match and position in text
                        confidence = self._calculate_confidence(position, chunk.start_char, len(text))
                        candidates.append({
                            'position': position,
                            'confidence': confidence,
                            'method': 'spacy_noun_phrase',
                            'source': 'body'
                        })
            
            # Return highest confidence candidate
            if candidates:
                candidates.sort(key=lambda x: x['confidence'], reverse=True)
                best = candidates[0]
                self.logger.debug(f"✓ SpaCy extracted position: {best['position']} (confidence: {best['confidence']:.2f})")
                return best['position']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in spacy position extraction: {str(e)}")
            return None
    
    def extract_all_job_positions(self, text: str, subject: str = None) -> List[Dict]:
        """
        Extract all job positions from text with metadata
        
        Args:
            text: Email body text
            subject: Email subject line
            
        Returns:
            List of dicts with {job_position, method, confidence, source}
        """
        positions = []
        seen = set()
        
        try:
            # 1. Try subject line first (often contains job title)
            if subject:
                subject_position = self.extract_job_position_regex(subject)
                if subject_position:
                    position_key = subject_position.lower()
                    if position_key not in seen:
                        positions.append({
                            'job_position': subject_position,
                            'method': 'regex',
                            'confidence': 0.90,  # High confidence for subject line
                            'source': 'subject'
                        })
                        seen.add(position_key)
            
            # 2. Try regex on body
            body_position_regex = self.extract_job_position_regex(text)
            if body_position_regex:
                position_key = body_position_regex.lower()
                if position_key not in seen:
                    positions.append({
                        'job_position': body_position_regex,
                        'method': 'regex',
                        'confidence': 0.80,
                        'source': 'body'
                    })
                    seen.add(position_key)
            
            # 3. Try spacy on body
            body_position_spacy = self.extract_job_position_spacy(text)
            if body_position_spacy:
                position_key = body_position_spacy.lower()
                if position_key not in seen:
                    positions.append({
                        'job_position': body_position_spacy,
                        'method': 'spacy',
                        'confidence': 0.70,
                        'source': 'body'
                    })
                    seen.add(position_key)
            
            # Sort by confidence
            positions.sort(key=lambda x: x['confidence'], reverse=True)
            
            return positions
            
        except Exception as e:
            self.logger.error(f"Error extracting all positions: {str(e)}")
            return positions
    
    def _clean_position(self, position: str) -> str:
        """Clean and normalize job position text"""
        if not position:
            return position
        
        # 1. Strip HTML/XML tags (e.g., <b>, </b>, <B>, </B>)
        position = re.sub(r'<[^>]+>', '', position)
        
        # 2. Remove marketing/fluff words
        position_lower = position.lower()
        for fluff in self.marketing_words:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(fluff) + r'\b'
            position = re.sub(pattern, '', position, flags=re.IGNORECASE)
        
        # 3. Remove common prefixes
        position_lower = position.lower()
        for prefix in self.prefixes_to_remove:
            if position_lower.startswith(prefix):
                position = position[len(prefix):]
                position_lower = position.lower()
        
        # 4. Remove trailing artifacts (location, duration, etc.)
        for artifact in self.trailing_artifacts:
            # Remove if it's the last word
            pattern = r'\s+' + re.escape(artifact) + r'$'
            position = re.sub(pattern, '', position, flags=re.IGNORECASE)
        
        # 5. Remove trailing words like "position", "role", "opening"
        position = re.sub(r'\s+(position|role|opening|opportunity|job)$', '', position, flags=re.IGNORECASE)
        
        # 6. Remove extra whitespace (including multiple spaces)
        position = ' '.join(position.split())
        
        # 7. Title case
        position = position.title()
        
        # 8. Handle common abbreviations
        position = position.replace('Sr.', 'Senior')
        position = position.replace('Jr.', 'Junior')
        position = position.replace('Mgr', 'Manager')
        
        # 9. Fix common patterns
        # "And" at the beginning (from removing "Highly Skilled And...")
        position = re.sub(r'^And\s+', '', position, flags=re.IGNORECASE)
        
        return position.strip()
    
    def _is_valid_position(self, position: str) -> bool:
        """Validate if text looks like a job position"""
        if not position:
            return False
        
        # Length check (2-100 chars)
        if len(position) < 2 or len(position) > 100:
            return False
        
        # Must have at least one letter
        if not any(c.isalpha() for c in position):
            return False
        
        # Must not be all uppercase (likely acronym or company name)
        if position.isupper() and len(position) > 10:
            return False
        
        # Check if it contains a job title suffix or trigger word
        position_lower = position.lower()
        
        has_suffix = any(suffix in position_lower for suffix in self.job_title_suffixes)
        has_trigger = any(trigger in position_lower for trigger in self.job_title_keywords)
        
        if not (has_suffix or has_trigger):
            return False
        
        # Filter out common false positives
        false_positives = [
            'team', 'department', 'company', 'organization', 'group',
            'please', 'thank', 'regards', 'sincerely', 'best',
            'email', 'phone', 'contact', 'address'
        ]
        
        if any(fp in position_lower for fp in false_positives):
            return False
        
        return True
    
    def _calculate_confidence(self, position: str, char_position: int, text_length: int) -> float:
        """
        Calculate confidence score for extracted position
        
        Args:
            position: Extracted position text
            char_position: Character position in text
            text_length: Total text length
            
        Returns:
            Confidence score (0.0 - 1.0)
        """
        confidence = 0.5  # Base confidence
        
        # Bonus for position early in text (first 500 chars)
        if char_position < 500:
            confidence += 0.2
        elif char_position < 1000:
            confidence += 0.1
        
        # Bonus for having job title suffix
        position_lower = position.lower()
        if any(suffix in position_lower for suffix in self.job_title_suffixes):
            confidence += 0.15
        
        # Bonus for multiple trigger words
        trigger_count = sum(1 for trigger in self.job_title_keywords if trigger in position_lower)
        if trigger_count >= 2:
            confidence += 0.1
        elif trigger_count == 1:
            confidence += 0.05
        
        # Penalty for very long positions (likely false positive)
        if len(position) > 60:
            confidence -= 0.1
        
        return max(0.0, min(1.0, confidence))
