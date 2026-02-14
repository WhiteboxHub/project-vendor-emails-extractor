"""
Run Summary JSON Logger

Creates detailed JSON summary for each extraction run with statistics.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List


class RunSummary:
    """Track and save detailed run statistics to JSON"""
    
    def __init__(self, output_dir='logs/runs'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize summary data
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.summary = {
            'run_id': self.run_id,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration_seconds': None,
            'total_candidates_processed': 0,
            'total_emails_fetched': 0,
            'total_contacts_inserted': 0,
            'total_contacts_extracted': 0,
            'total_duplicates_skipped': 0,
            'total_successful': 0,
            'total_failed': 0,
            'failed_candidate_emails': [],
            'candidates': [],
            'filter_stats': {
                'total_emails': 0,
                'passed': 0,
                'junk': 0,
                'not_recruiter': 0,
                'calendar_invites': 0
            },
            'extraction_methods': {
                'regex': 0,
                'spacy': 0,
                'gliner': 0
            },
            'errors': []
        }
        
        self.start_time = datetime.now()
    
    def add_candidate_result(self, candidate_email: str, result: Dict):
        """
        Add candidate processing result
        
        Args:
            candidate_email: Candidate email address
            result: Dictionary with processing results
                - emails_fetched: int
                - contacts_extracted: int
                - contacts_inserted: int
                - duplicates_skipped: int
                - filter_stats: dict
                - error: str (optional)
        """
        candidate_data = {
            'email': candidate_email,
            'emails_fetched': result.get('emails_fetched', 0),
            'contacts_extracted': result.get('contacts_extracted', 0),
            'contacts_inserted': result.get('contacts_inserted', 0),
            'duplicates_skipped': result.get('duplicates_skipped', 0),
            'filter_stats': result.get('filter_stats', {}),
            'error': result.get('error'),
            'processed_at': datetime.now().isoformat()
        }
        
        self.summary['candidates'].append(candidate_data)
        
        # Update totals
        self.summary['total_candidates_processed'] += 1
        self.summary['total_emails_fetched'] += result.get('emails_fetched', 0)
        self.summary['total_contacts_extracted'] += result.get('contacts_extracted', 0)
        self.summary['total_contacts_inserted'] += result.get('contacts_inserted', 0)
        self.summary['total_duplicates_skipped'] += result.get('duplicates_skipped', 0)
        
        # Aggregate filter stats
        filter_stats = result.get('filter_stats', {})
        for key in self.summary['filter_stats']:
            self.summary['filter_stats'][key] += filter_stats.get(key, 0)
        
        # Track errors
        if result.get('error'):
            self.summary['total_failed'] += 1
            self.summary['failed_candidate_emails'].append(candidate_email)
            self.summary['errors'].append({
                'candidate': candidate_email,
                'error': result['error'],
                'timestamp': datetime.now().isoformat()
            })
        else:
            self.summary['total_successful'] += 1
    
    def add_extraction_method_count(self, method: str, count: int = 1):
        """Track which extraction method was used"""
        if method in self.summary['extraction_methods']:
            self.summary['extraction_methods'][method] += count
    
    def finalize(self) -> str:
        """
        Finalize and save the run summary
        
        Returns:
            Path to saved JSON file
        """
        end_time = datetime.now()
        self.summary['end_time'] = end_time.isoformat()
        self.summary['duration_seconds'] = (end_time - self.start_time).total_seconds()
        
        # Calculate success rate
        if self.summary['total_candidates_processed'] > 0:
            successful = self.summary['total_candidates_processed'] - len(self.summary['errors'])
            self.summary['success_rate'] = f"{(successful / self.summary['total_candidates_processed'] * 100):.1f}%"
        else:
            self.summary['success_rate'] = "0%"
        
        # Save to JSON file
        filename = f"run_summary_{self.run_id}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.summary, f, indent=2, ensure_ascii=False)
        
        return str(filepath)
    
    def get_summary(self) -> Dict:
        """Get current summary data"""
        return self.summary.copy()
    
    def print_summary(self):
        """Print formatted summary to console"""
        print("\n" + "=" * 80)
        print(f"RUN SUMMARY - {self.run_id}")
        print("=" * 80)
        print(f"Duration: {self.summary.get('duration_seconds', 0):.1f}s")
        print(f"Candidates Processed: {self.summary['total_candidates_processed']}")
        print(f"  - Successful: {self.summary['total_successful']}")
        print(f"  - Failed:     {self.summary['total_failed']}")
        print(f"Success Rate: {self.summary.get('success_rate', 'N/A')}")
        print(f"\nEmails Fetched: {self.summary['total_emails_fetched']}")
        print(f"Contacts Extracted: {self.summary['total_contacts_extracted']}")
        print(f"Contacts Inserted: {self.summary['total_contacts_inserted']}")
        print(f"Duplicates Skipped: {self.summary['total_duplicates_skipped']}")
        print(f"\nFilter Stats:")
        print(f"  - Total: {self.summary['filter_stats']['total_emails']}")
        print(f"  - Passed: {self.summary['filter_stats']['passed']}")
        print(f"  - Junk: {self.summary['filter_stats']['junk']}")
        print(f"  - Not Recruiter: {self.summary['filter_stats']['not_recruiter']}")
        print(f"  - Calendar Invites: {self.summary['filter_stats']['calendar_invites']}")
        
        if self.summary['failed_candidate_emails']:
            print(f"\nFailed Candidates ({len(self.summary['failed_candidate_emails'])}):")
            for email in self.summary['failed_candidate_emails']:
                print(f"  - {email}")
        
        if self.summary['errors']:
            print(f"\nError Details (First 5):")
            for error in self.summary['errors'][:5]:  # Show first 5
                print(f"  - {error['candidate']}: {error['error']}")
        
        print("=" * 80 + "\n")
