import logging
import torch
import os
from transformers import pipeline
import time

logger = logging.getLogger(__name__)

class BertPositionClassifier:
    """
    Classifier using BERT to validate job positions.
    Supports either a custom fine-tuned binary model or a zero-shot fallback.
    """
    
    def __init__(self, model_name: str = "valhalla/distilbart-mnli-12-1", device: int = -1):
        """
        Initialize classifier. 
        Auto-detects if a custom binary model exists.
        """
        self.logger = logging.getLogger(__name__)
        self.device = device
        self.classifier = None
        self.model_type = "zero-shot" # Default
        
        # Check for custom binary model
        # Try relative path first (common for batch scripts)
        custom_model_path = "models/bert_binary_classifier"
        # Also try absolute if possible or relative to current file
        if not os.path.exists(custom_model_path):
             # Try relative to the script location
             base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
             custom_model_path = os.path.join(base_dir, "models", "bert_binary_classifier")

        if os.path.exists(custom_model_path):
            self.logger.info(f"Found custom binary model at {custom_model_path}")
            model_name = custom_model_path
            self.model_type = "binary"
        else:
            self.logger.info(f"No custom model found. Using Zero-Shot: {model_name}")

        try:
            start_time = time.time()
            if device == -1 and torch.cuda.is_available():
                self.device = 0
                self.logger.info("CUDA available - using GPU")
            
            if self.model_type == "binary":
                self.classifier = pipeline(
                    "text-classification",
                    model=model_name,
                    device=self.device
                )
            else:
                self.classifier = pipeline(
                    "zero-shot-classification",
                    model=model_name,
                    device=self.device
                )
                # Labels for Zero-Shot
                self.candidate_labels = ["job title", "marketing spam", "junk text"]
                self.valid_labels = ["job title"]
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Model loaded in {elapsed:.2f}s (Mode: {self.model_type.upper()})")
            
        except Exception as e:
            self.logger.error(f"Failed to load BERT classifier: {str(e)}")
            raise

    def classify(self, text: str) -> dict:
        """
        Classify text
        
        Args:
            text: Text to classify (Title + Context)
            
        Returns:
            Dict with 'label', 'score', 'is_valid'
        """
        if not self.classifier or not text:
            return {'label': 'unknown', 'score': 0.0, 'is_valid': False}
        
        try:
            # Handle text length (BERT limit 512 tokens)
            if len(text) > 1000:
                text = text[:1000]

            if self.model_type == "binary":
                # Binary Classification Output: [{'label': 'LABEL_1', 'score': 0.99}]
                result = self.classifier(text)[0]
                label = result['label']
                score = result['score']
                
                # Default mapping from Trainer (LABEL_1 = Valid, LABEL_0 = Junk)
                is_valid = (label == 'LABEL_1' or label == '1' or label == 1)
                friendly_label = "valid_job" if is_valid else "junk"
                
                return {
                    'label': friendly_label,
                    'score': score,
                    'is_valid': is_valid
                }
            
            else:
                # Zero-Shot Logic (Fallback)
                result = self.classifier(
                    text, 
                    self.candidate_labels, 
                    multi_label=False
                )
                top_label = result['labels'][0]
                top_score = result['scores'][0]
                is_valid = top_label in self.valid_labels
                
                return {
                    'label': top_label,
                    'score': top_score,
                    'is_valid': is_valid
                }
            
        except Exception as e:
            self.logger.error(f"Classification error: {str(e)}")
            return {'label': 'error', 'score': 0.0, 'is_valid': False}

    def batch_classify(self, texts: list) -> list:
        """Classify a list of texts"""
        return [self.classify(t) for t in texts]
