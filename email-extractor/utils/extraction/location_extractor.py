"""
Location Extractor - Extract locations and zip codes from recruiter emails

This module provides methods for extracting:
1. Locations (city, state)
2. Zip/postal codes (US, Canada, UK)
3. Combined location with zip parsing
"""

import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


class LocationExtractor:
    """Extract locations and zip codes from email text"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # US ZIP code patterns
        self.us_zip_pattern = re.compile(
            r'\b(\d{5}(?:-\d{4})?)\b'  # 12345 or 12345-6789
        )
        
        # Canada postal code pattern
        self.canada_zip_pattern = re.compile(
            r'\b([A-Z]\d[A-Z]\s?\d[A-Z]\d)\b',  # A1A 1A1 or A1A1A1
            re.IGNORECASE
        )
        
        # UK postal code pattern
        self.uk_zip_pattern = re.compile(
            r'\b([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})\b',  # SW1A 1AA, EC1A 1BB, etc.
            re.IGNORECASE
        )
        
        # Location patterns (City, State ZIP)
        self.location_patterns = [
            # "City, ST 12345" or "City, State 12345"
            r'([A-Z][a-zA-Z\s]+),\s*([A-Z]{2}|\b(?:Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b)\s*(\d{5}(?:-\d{4})?)?',
            
            # "Location: City, ST" or "Location: City, State"
            r'(?:Location|City|Based in|Located in):\s*([A-Z][a-zA-Z\s]+),\s*([A-Z]{2}|\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(\d{5}(?:-\d{4})?)?',
            
            # "City, ST" (simple format)
            r'\b([A-Z][a-zA-Z\s]{2,30}),\s*([A-Z]{2})\b',
        ]
        
        # US State abbreviations for validation
        self.us_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
        }
        
        # State name to abbreviation mapping
        self.state_name_to_abbr = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
            'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
            'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
            'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
            'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
            'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
            'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
            'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
            'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
            'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
            'wisconsin': 'WI', 'wyoming': 'WY'
        }
    
    def extract_zip_code(self, text: str) -> Optional[str]:
        """
        Extract ZIP/postal code from text
        
        Args:
            text: Email body text
            
        Returns:
            Extracted zip code or None
        """
        try:
            if not text:
                return None
            
            # Try US ZIP first (most common)
            match = self.us_zip_pattern.search(text)
            if match:
                zip_code = match.group(1)
                # Validate it's not a phone number or other number
                if self._is_valid_us_zip(zip_code):
                    self.logger.debug(f"✓ Extracted US ZIP: {zip_code}")
                    return zip_code
            
            # Try Canada postal code
            match = self.canada_zip_pattern.search(text)
            if match:
                zip_code = match.group(1).upper()
                self.logger.debug(f"✓ Extracted Canada postal code: {zip_code}")
                return zip_code
            
            # Try UK postal code
            match = self.uk_zip_pattern.search(text)
            if match:
                zip_code = match.group(1).upper()
                self.logger.debug(f"✓ Extracted UK postal code: {zip_code}")
                return zip_code
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting zip code: {str(e)}")
            return None
    
    def extract_location_with_zip(self, text: str) -> Dict[str, Optional[str]]:
        """
        Extract location with city, state, and zip code
        
        Args:
            text: Email body text
            
        Returns:
            Dict with keys: location, city, state, zip_code
        """
        result = {
            'location': None,
            'city': None,
            'state': None,
            'zip_code': None
        }
        
        try:
            if not text:
                return result
            
            # Try each location pattern
            for pattern in self.location_patterns:
                matches = re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE)
                
                for match in matches:
                    city = match.group(1).strip() if match.lastindex >= 1 else None
                    state = match.group(2).strip() if match.lastindex >= 2 else None
                    zip_code = match.group(3).strip() if match.lastindex >= 3 else None
                    
                    # Validate and normalize
                    if city and state:
                        # Normalize state to abbreviation
                        state_normalized = self._normalize_state(state)
                        
                        if state_normalized:
                            # Clean city name
                            city_clean = self._clean_city_name(city)
                            
                            if city_clean:
                                # Build location string
                                location = f"{city_clean}, {state_normalized}"
                                
                                # Validate zip if present
                                if zip_code and not self._is_valid_us_zip(zip_code):
                                    zip_code = None
                                
                                result['location'] = location
                                result['city'] = city_clean
                                result['state'] = state_normalized
                                result['zip_code'] = zip_code
                                
                                self.logger.debug(f"✓ Extracted location: {location}" + 
                                                (f" {zip_code}" if zip_code else ""))
                                return result
            
            # If no structured location found, try to extract zip code separately
            if not result['zip_code']:
                result['zip_code'] = self.extract_zip_code(text)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting location with zip: {str(e)}")
            return result
    
    def parse_location_components(self, location: str) -> Dict[str, Optional[str]]:
        """
        Parse location string into components
        
        Args:
            location: Location string (e.g., "San Francisco, CA 94105")
            
        Returns:
            Dict with keys: city, state, zip_code
        """
        result = {
            'city': None,
            'state': None,
            'zip_code': None
        }
        
        try:
            if not location:
                return result
            
            # Pattern: "City, ST ZIP" or "City, State ZIP"
            match = re.match(
                r'([^,]+),\s*([A-Z]{2}|\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*(\d{5}(?:-\d{4})?)?',
                location.strip()
            )
            
            if match:
                city = match.group(1).strip()
                state = match.group(2).strip()
                zip_code = match.group(3).strip() if match.group(3) else None
                
                # Normalize
                state_normalized = self._normalize_state(state)
                city_clean = self._clean_city_name(city)
                
                if city_clean and state_normalized:
                    result['city'] = city_clean
                    result['state'] = state_normalized
                    result['zip_code'] = zip_code
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing location components: {str(e)}")
            return result
    
    def _normalize_state(self, state: str) -> Optional[str]:
        """Normalize state name to 2-letter abbreviation"""
        if not state:
            return None
        
        state = state.strip()
        
        # Already an abbreviation
        if len(state) == 2 and state.upper() in self.us_states:
            return state.upper()
        
        # Full state name
        state_lower = state.lower()
        if state_lower in self.state_name_to_abbr:
            return self.state_name_to_abbr[state_lower]
        
        return None
    
    def _clean_city_name(self, city: str) -> Optional[str]:
        """Clean and validate city name"""
        if not city:
            return None
        
        # Remove extra whitespace
        city = ' '.join(city.split())
        
        # Title case
        city = city.title()
        
        # Validate length
        if len(city) < 2 or len(city) > 50:
            return None
        
        # Must have letters
        if not any(c.isalpha() for c in city):
            return None
        
        # Filter out common false positives
        false_positives = [
            'team', 'department', 'company', 'position', 'role',
            'email', 'phone', 'contact', 'address', 'street'
        ]
        
        if city.lower() in false_positives:
            return None
        
        return city
    
    def _is_valid_us_zip(self, zip_code: str) -> bool:
        """Validate US ZIP code"""
        if not zip_code:
            return False
        
        # Must be 5 digits or 5+4 format
        if not re.match(r'^\d{5}(?:-\d{4})?$', zip_code):
            return False
        
        # First digit should be 0-9 (all valid)
        # But we can add more validation if needed
        
        # Avoid common false positives (years, etc.)
        if zip_code.startswith('19') or zip_code.startswith('20'):
            # Could be a year, need more context
            # For now, we'll allow it but with lower confidence
            pass
        
        return True
