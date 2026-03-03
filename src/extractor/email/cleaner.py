import re
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class EmailCleaner:
    """Clean and sanitize email content for extraction"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_html(self, html_content: str) -> str:
        """
        Remove HTML tags and extract clean text
        
        Args:
            html_content: Raw HTML email content
            
        Returns:
            Clean text content
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text with newlines preserved
            text = soup.get_text(separator="\n")
            
            # Remove quoted replies
            text = self._remove_quoted_replies(text)
            
            # Normalize whitespace
            text = self._normalize_whitespace(text)
            
            return text.strip()
            
        except Exception as e:
            self.logger.error(f"Error cleaning HTML: {str(e)}")
            return html_content
    
    def _remove_quoted_replies(self, text: str) -> str:
        """Remove quoted email replies and forwarded messages"""
        # Common reply patterns (split at first occurrence)
        split_patterns = [
            r"On .+ wrote:",
            r"From:.+Sent:.+To:.+Subject:",
            r"_{5,}",  # Long underscores (email separators)
            r"-{5,}",  # Long dashes (email separators)
            r"Begin forwarded message:",
            r"\bwrote:\s*$",  # Standalone "wrote:" at end of line
        ]

        for pattern in split_patterns:
            parts = re.split(pattern, text, maxsplit=1, flags=re.MULTILINE)
            text = parts[0]

        # Strip lines that begin with ">" (standard plain-text quoted reply format)
        # e.g.  > On Mon, Jan 1 John Doe <john@x.com> wrote:
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith('>'):
                # Once we hit a quoted block, stop — everything below is reply context
                break
            cleaned_lines.append(line)
        text = '\n'.join(cleaned_lines)

        # Remove "Sent from my iPhone / Android / …" device signatures (common in plain-text)
        text = re.sub(
            r'\n\s*Sent from my (iPhone|Android|iPad|Samsung|BlackBerry|Windows Phone|mobile device)[^\n]*',
            '',
            text,
            flags=re.IGNORECASE
        )

        return text
    
    def _normalize_whitespace(self, text: str) -> str:
        """Normalize excessive whitespace and blank lines"""
        # Replace multiple spaces with single space
        text = re.sub(r' +', ' ', text)
        
        # Replace multiple newlines with max 2
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove trailing/leading whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        
        return '\n'.join(lines)
    
    def extract_body(self, email_message) -> str:
        """
        Extract email body from message object with better cleaning
        
        Args:
            email_message: Email message object
            
        Returns:
            Clean email body text
        """
        body = ""
        
        try:
            if email_message.is_multipart():
                text_body = None
                html_body = None
                
                for part in email_message.walk():
                    content_type = part.get_content_type()
                    
                    # Get plain text
                    if content_type == 'text/plain':
                        payload = part.get_payload(decode=True)
                        if payload:
                            text_body = payload.decode('utf-8', errors='ignore')
                    
                    # Get HTML
                    elif content_type == 'text/html':
                        payload = part.get_payload(decode=True)
                        if payload:
                            html_body = payload.decode('utf-8', errors='ignore')
                
                # Prefer text, fallback to HTML
                body = text_body if text_body else (self.clean_html(html_body) if html_body else "")
            else:
                payload = email_message.get_payload(decode=True)
                if payload:
                    body = payload.decode('utf-8', errors='ignore')
                    if '<html' in body.lower():
                        body = self.clean_html(body)
            
            # Final cleaning
            if body:
                body = self._remove_quoted_replies(body)
                body = self._normalize_whitespace(body)
                # Limit length (GLiNER works best with <2000 chars)
                body = body[:3000]  # Keep first 3000 chars
            
            return body.strip()
            
        except Exception as e:
            self.logger.error(f"Error extracting email body: {str(e)}")
            return ""
