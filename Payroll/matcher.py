import unicodedata
import re
from typing import Dict, List, Tuple
from dataclasses import dataclass

try:
    from rapidfuzz import fuzz
except ImportError:
    from fuzzywuzzy import fuzz


@dataclass
class MatchResult:
    harvest_user_id: str
    deel_contract_id: str
    confidence: float
    signals: Dict
    decision: str  # 'auto_accept', 'needs_review', 'auto_reject'


class UserMatcher:
    def __init__(self, auto_accept_threshold: float = 0.90, review_threshold: float = 0.70):
        """
        Initialize matcher with confidence thresholds.

        Args:
            auto_accept_threshold: Confidence above this = auto-match (default 90%)
            review_threshold: Confidence above this = queue for review (default 70%)
        """
        self.auto_accept = auto_accept_threshold
        self.review_threshold = review_threshold

    def normalize_name(self, name: str) -> str:
        """Normalize name for comparison: lowercase, remove accents, extra spaces."""
        if not name:
            return ""

        # Remove accents (José → Jose)
        name = unicodedata.normalize('NFKD', name)
        name = name.encode('ascii', 'ignore').decode('utf-8')

        # Lowercase and clean
        name = name.lower().strip()
        name = re.sub(r'\s+', ' ', name)  # Multiple spaces → single space
        name = re.sub(r'[^\w\s\-]', '', name)  # Remove special chars except hyphen

        return name

    def normalize_email(self, email: str) -> str:
        """Normalize email: lowercase, strip whitespace."""
        if not email:
            return ""
        return email.lower().strip()

    def compute_name_similarity(self, name1: str, name2: str) -> Dict:
        """
        Compute name similarity using multiple algorithms.
        Returns dict with scores and method used.
        """
        n1 = self.normalize_name(name1)
        n2 = self.normalize_name(name2)

        if not n1 or not n2:
            return {"score": 0, "method": "missing_data"}

        # Algorithm 1: Simple ratio
        simple_score = fuzz.ratio(n1, n2) / 100

        # Algorithm 2: Token sort (handles "John Smith" vs "Smith, John")
        token_score = fuzz.token_sort_ratio(n1, n2) / 100

        # Algorithm 3: Token set (handles middle names, initials)
        token_set_score = fuzz.token_set_ratio(n1, n2) / 100

        # Algorithm 4: Partial ratio (handles "Bob" vs "Robert Smith")
        partial_score = fuzz.partial_ratio(n1, n2) / 100

        # Take the best score
        best_score = max(simple_score, token_score, token_set_score, partial_score)

        return {
            "score": best_score,
            "simple": simple_score,
            "token_sort": token_score,
            "token_set": token_set_score,
            "partial": partial_score,
            "method": "fuzzy_name_match"
        }

    def match_user(self, harvest_user: Dict, deel_contract: Dict) -> MatchResult:
        """
        Match a single Harvest user against a single Deel contract.

        Args:
            harvest_user: dict with keys: id, email, first_name, last_name
            deel_contract: dict with keys: id, title, worker.full_name, worker.email

        Returns:
            MatchResult with confidence score and decision
        """
        signals = {}
        weights = {}

        # Signal 1: Email match (strongest signal)
        harvest_email = self.normalize_email(harvest_user.get('email', ''))

        # Deel stores email in worker object
        worker = deel_contract.get('worker') or {}
        deel_email = self.normalize_email(worker.get('email', ''))

        if harvest_email and deel_email and harvest_email == deel_email:
            signals['email_match'] = 1.0
            weights['email_match'] = 5.0  # Very high weight
        else:
            signals['email_match'] = 0.0
            weights['email_match'] = 0.5  # Lower weight when emails don't match (reduced from 1.0)

        # Signal 2: Name similarity
        # Check BOTH contract title AND worker.full_name (important for "Untitled Contract" cases)
        harvest_name = f"{harvest_user.get('first_name', '')} {harvest_user.get('last_name', '')}"

        # Get all possible names from Deel contract
        deel_title = deel_contract.get('title', '')
        deel_worker_name = worker.get('full_name', '')

        # Compute similarity against both
        title_result = self.compute_name_similarity(harvest_name, deel_title)
        worker_result = self.compute_name_similarity(harvest_name, deel_worker_name)

        # Use the BEST name match
        if worker_result['score'] > title_result['score']:
            name_result = worker_result
            signals['matched_against'] = 'worker.full_name'
        else:
            name_result = title_result
            signals['matched_against'] = 'title'

        signals['name_similarity'] = name_result['score']
        signals['name_details'] = name_result
        weights['name_similarity'] = 5.0

        # Calculate weighted confidence score
        total_weight = sum(weights.values())
        confidence = sum(
            signals[k] * w for k, w in weights.items()
            if k in signals and isinstance(signals[k], (int, float))
        ) / total_weight

        # Make decision based on thresholds
        # SPECIAL CASE: If name matches very closely (95%+), auto-accept even without email
        if signals['name_similarity'] >= 0.95:
            decision = 'auto_accept'
        elif confidence >= self.auto_accept:
            decision = 'auto_accept'
        elif confidence >= self.review_threshold:
            decision = 'needs_review'
        else:
            decision = 'auto_reject'

        return MatchResult(
            harvest_user_id=str(harvest_user['id']),
            deel_contract_id=deel_contract['id'],
            confidence=confidence,
            signals=signals,
            decision=decision
        )

    def find_best_match(self, harvest_user: Dict, deel_contracts: List[Dict]) -> MatchResult:
        """
        Find the best matching Deel contract for a Harvest user.

        Args:
            harvest_user: Harvest user dict
            deel_contracts: List of all active Deel contracts

        Returns:
            Best MatchResult, or None if no reasonable match found
        """
        # Only consider active contracts
        active_contracts = [
            c for c in deel_contracts
            if c.get('status') == 'in_progress'
        ]

        if not active_contracts:
            return None

        # Match against all contracts
        all_matches = [
            self.match_user(harvest_user, contract)
            for contract in active_contracts
        ]

        # Sort by confidence (highest first)
        all_matches.sort(key=lambda x: x.confidence, reverse=True)

        # Return best match (or None if best is auto_reject)
        best = all_matches[0]
        return best if best.decision != 'auto_reject' else None