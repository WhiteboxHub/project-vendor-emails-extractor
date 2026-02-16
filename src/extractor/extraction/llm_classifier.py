import logging
import os
import json
import re
from typing import Dict, List, Optional
from groq import Groq

logger = logging.getLogger(__name__)

class LLMJobClassifier:
    """
    Classifier using Groq API (Llama 3.1) to validate job positions.
    Uses generative prompting to provide reasoning and labels.
    """
    
    def __init__(
        self, 
        model_name: str = "llama-3.1-8b-instant", 
        threshold: float = 0.7
    ):
        self.logger = logging.getLogger(__name__)
        self.threshold = threshold
        self.model_name = model_name
        
        # Initialize Groq
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            self.logger.error("GROQ_API_KEY not found in environment variables")
            # We don't raise here to allow the orchestrator to handle the missing key gracefully if needed
            # but classify will fail later if not fixed.
            
        self.client = Groq(api_key=api_key) if api_key else None
        self.logger.info(f"✓ Groq LLM initialized with model: {model_name}")

    def build_system_prompt(self) -> str:
        """
        Constructs the system instruction for classification.
        """
        return (
            "You are an expert recruitment assistant specializing in data quality.\n"
            "Your task is to categorize a text block as either a 'valid_job' or 'junk'.\n\n"
            "STRICT CRITERIA:\n"
            "1. 'valid_job':\n"
            "   - Must describe a specific, open job position.\n"
            "   - Must include a title and some requirements or duties.\n\n"
            "2. 'junk':\n"
            "   - Text that is purely an email signature, footer, or legal disclaimer.\n"
            "   - General company advertisements without a specific role.\n"
            "   - Newsletters, spam, or broken text fragments.\n\n"
            "OUTPUT FORMAT:\n"
            "You must respond ONLY with a valid JSON object. Do not include any other text.\n"
            "Example JSON:\n"
            "{\n"
            "  \"reasoning\": \"One sentence explanation.\",\n"
            "  \"label\": \"valid_job\" or \"junk\",\n"
            "  \"confidence\": 0.0 to 1.0\n"
            "}"
        )

    def classify(self, text: str) -> Dict:
        """
        Perform Groq-based classification.
        """
        if not text:
            return {'label': 'empty', 'score': 0.0, 'is_valid': False}

        if not self.client:
            return {'label': 'error', 'score': 0.0, 'is_valid': False, 'reason': 'GROQ_API_KEY missing'}

        try:
            # Truncate text to keep prompt within reasonable limits
            # Llama 3.1 has a large context, but let's be efficient
            if len(text) > 4000:
                text = text[:4000]

            completion = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": self.build_system_prompt()},
                    {"role": "user", "content": f"Classify this text:\n\n{text}"}
                ],
                temperature=0.0,
                max_tokens=200,
                response_format={"type": "json_object"}
            )
            
            output_text = completion.choices[0].message.content.strip()

            # Parse JSON from response
            result = json.loads(output_text)

            logger.info(f"LLM Classification reasoning for raw job ID 'reasoning': {result.get('reasoning', '')}")

            label = result.get('label', 'junk').lower()
            score = float(result.get('confidence', 0.5))
            
            # Check for valid_job label
            is_valid = (label == 'valid_job') and (score >= self.threshold)
            
            return {
                'label': "valid" if is_valid else "junk",
                'score': score,
                'is_valid': is_valid,
                'reasoning': result.get('reasoning', ''),
                'raw_llm_output': output_text
            }
            
        except Exception as e:
            self.logger.error(f"Groq LLM Classification error: {e}")
            return {'label': 'error', 'score': 0.0, 'is_valid': False}

    def batch_classify(self, texts: List[str]) -> List[Dict]:
        return [self.classify(t) for t in texts]
