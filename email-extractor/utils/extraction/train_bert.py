import os
import logging
import torch
import pandas as pd
from sklearn.model_selection import train_test_split
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    Trainer,
    TrainingArguments
)
from torch.utils.data import Dataset

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobDataset(Dataset):
    """Custom Dataset for Job Classification"""
    def __init__(self, texts, labels, tokenizer, max_len=128):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        label = int(self.labels[idx])

        encoding = self.tokenizer(
            text,
            add_special_tokens=True,
            max_length=self.max_len,
            padding='max_length',
            truncation=True,
            return_attention_mask=True,
            return_tensors='pt',
        )

        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'labels': torch.tensor(label, dtype=torch.long)
        }

def train_model(data_path='labeled_data.csv', output_dir='models/bert_binary_classifier'):
    """
    Train Binary BERT Classifier
    Data format: CSV with columns 'text' and 'label' (0=Junk, 1=Valid)
    """
    if not os.path.exists(data_path):
        logger.error(f"Data file not found: {data_path}")
        print("Please provide a CSV with columns 'text' and 'label'")
        return

    logger.info(f"Loading data from {data_path}...")
    df = pd.read_csv(data_path)
    
    if 'text' not in df.columns or 'label' not in df.columns:
         logger.error("CSV must have 'text' and 'label' columns")
         return
         
    # Split data
    train_texts, val_texts, train_labels, val_labels = train_test_split(
        df['text'], df['label'], test_size=0.2, random_state=42
    )
    
    # Initialize Tokenizer & Model
    model_name = 'distilbert-base-uncased'
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2)
    
    # Create Datasets
    train_dataset = JobDataset(train_texts.tolist(), train_labels.tolist(), tokenizer)
    val_dataset = JobDataset(val_texts.tolist(), val_labels.tolist(), tokenizer)
    
    # Training Arguments
    training_args = TrainingArguments(
        output_dir='./results',
        num_train_epochs=5,
        per_device_train_batch_size=2, # Even smaller
        gradient_accumulation_steps=4, # Accumulate gradients to keep effective batch size but lower memory
        warmup_steps=0,
        weight_decay=0.01,
        logging_steps=1,
        eval_strategy="no",
        save_strategy="no",
        load_best_model_at_end=False,
        use_cpu=True
    )
    
    # Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset
    )
    
    # Train
    logger.info("Starting training...")
    trainer.train()
    
    # Save Model
    logger.info(f"Saving model to {output_dir}...")
    model.save_pretrained(output_dir)
    tokenizer.save_pretrained(output_dir)
    logger.info("Training completed successfully!")

if __name__ == "__main__":
    train_model()
