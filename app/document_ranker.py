"""
Document Relevance Ranking for Climate Risk Assessment

Ranks search results by relevance to physical climate risk assessment,
prioritizing official reports, sustainability disclosures, and physical risk content.
"""

import re
from typing import List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DocumentRanker:
    """Ranks documents by relevance for climate risk assessment"""
    
    # Tier 1: Official Sustainability Reports (highest priority)
    TIER1_KEYWORDS = [
        'cdp', 'climate change response', 'tcfd', 'task force climate',
        'sustainability report', 'esg report', 'environmental social governance',
        'annual report', 'sec 10-k', 'form 10-k', 'gri report',
        'climate disclosure', 'climate-related financial disclosure'
    ]
    
    # Tier 2: Company Climate Commitments
    TIER2_KEYWORDS = [
        'net zero', 'net-zero', 'carbon neutral', 'climate commitment',
        'science based target', 'sbti', 'climate action plan',
        'climate strategy', 'decarbonization', 'emissions reduction target'
    ]
    
    # Tier 3: Physical Risk Specific
    TIER3_KEYWORDS = [
        'physical risk', 'physical climate risk', 'climate adaptation',
        'climate resilience', 'climate vulnerability', 'climate hazard',
        'extreme weather', 'flood risk', 'drought risk', 'heat stress',
        'water stress', 'sea level rise', 'wildfire risk', 'hurricane',
        'asset vulnerability', 'facility risk', 'supply chain risk'
    ]
    
    # Tier 4: General Climate Information
    TIER4_KEYWORDS = [
        'climate risk', 'climate change', 'climate impact',
        'environmental risk', 'sustainability', 'carbon emissions'
    ]
    
    # Measure-specific keywords for targeted retrieval
    MEASURE_KEYWORDS = {
        'governance': ['board oversight', 'management responsibility', 'governance', 'climate committee'],
        'strategy': ['risk assessment', 'scenario analysis', 'climate strategy', 'integration'],
        'risk_management': ['risk identification', 'risk management', 'risk process'],
        'metrics_targets': ['emissions', 'ghg', 'scope 1', 'scope 2', 'scope 3', 'targets', 'metrics'],
        'physical_acute': ['extreme weather', 'flood', 'hurricane', 'cyclone', 'wildfire', 'storm'],
        'physical_chronic': ['temperature', 'precipitation', 'sea level', 'drought', 'water stress', 'heat stress']
    }
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
    
    def rank_documents(self, documents: List[Dict], top_n: int = 50) -> List[Dict]:
        """
        Rank documents by relevance and return top N
        
        Args:
            documents: List of document dicts with 'title', 'description', 'url'
            top_n: Number of top documents to return
            
        Returns:
            List of top N documents sorted by relevance score
        """
        if not documents:
            return []
        
        # Score each document
        scored_docs = []
        for doc in documents:
            score = self._calculate_relevance_score(doc)
            scored_docs.append({
                **doc,
                'relevance_score': score
            })
        
        # Sort by score (descending) and return top N
        scored_docs.sort(key=lambda x: x['relevance_score'], reverse=True)
        top_docs = scored_docs[:top_n]
        
        if self.verbose:
            logger.info(f"Ranked {len(documents)} documents, selected top {len(top_docs)}")
            logger.info(f"Score range: {top_docs[0]['relevance_score']:.1f} to {top_docs[-1]['relevance_score']:.1f}")
        
        return top_docs
    
    def _calculate_relevance_score(self, doc: Dict) -> float:
        """Calculate relevance score for a single document"""
        
        # Combine title and description for analysis
        text = f"{doc.get('title', '')} {doc.get('description', '')}".lower()
        url = doc.get('url', '').lower()
        
        score = 0.0
        
        # Tier scoring (higher tier = higher base score)
        tier1_matches = sum(1 for kw in self.TIER1_KEYWORDS if kw in text or kw in url)
        tier2_matches = sum(1 for kw in self.TIER2_KEYWORDS if kw in text or kw in url)
        tier3_matches = sum(1 for kw in self.TIER3_KEYWORDS if kw in text or kw in url)
        tier4_matches = sum(1 for kw in self.TIER4_KEYWORDS if kw in text or kw in url)
        
        # Weighted tier scores
        score += tier1_matches * 100  # Official reports get highest weight
        score += tier2_matches * 50   # Commitments second
        score += tier3_matches * 30   # Physical risk specific third
        score += tier4_matches * 10   # General climate info lowest
        
        # Bonus for official sources
        if any(domain in url for domain in ['cdp.net', 'sec.gov', 'globalreporting.org']):
            score += 200
        
        # Bonus for company domain (more authoritative)
        if self._is_company_domain(url):
            score += 50
        
        # Bonus for PDF documents (usually official reports)
        if url.endswith('.pdf') or 'pdf' in url:
            score += 30
        
        # Recency bonus (if we can extract year from URL or title)
        year = self._extract_year(text + url)
        if year:
            current_year = datetime.now().year
            if year >= current_year - 1:  # Last 2 years
                score += 20
            elif year >= current_year - 3:  # Last 4 years
                score += 10
        
        # Penalty for news/blog sites (less authoritative)
        if any(domain in url for domain in ['news', 'blog', 'medium.com', 'forbes.com']):
            score -= 20
        
        return score
    
    def _is_company_domain(self, url: str) -> bool:
        """Check if URL is from company's own domain"""
        # Heuristic: not a third-party platform
        third_party = ['cdp.net', 'sec.gov', 'news', 'blog', 'medium', 'forbes', 
                      'reuters', 'bloomberg', 'ft.com', 'wsj.com']
        return not any(domain in url for domain in third_party)
    
    def _extract_year(self, text: str) -> int:
        """Extract most recent year from text"""
        # Look for 4-digit years between 2015-2025
        years = re.findall(r'\b(20[1-2][0-9])\b', text)
        if years:
            return max(int(y) for y in years)
        return None
    
    def filter_for_measures(self, documents: List[Dict], measure_category: str, 
                           top_n: int = 20) -> List[Dict]:
        """
        Filter documents specifically relevant to a measure category
        
        Args:
            documents: List of all documents
            measure_category: Category like 'governance', 'physical_acute', etc.
            top_n: Number of documents to return
            
        Returns:
            Top N documents relevant to the measure category
        """
        if measure_category not in self.MEASURE_KEYWORDS:
            # If no specific keywords, just return top ranked
            return self.rank_documents(documents, top_n)
        
        keywords = self.MEASURE_KEYWORDS[measure_category]
        
        # Score documents by measure-specific relevance
        scored_docs = []
        for doc in documents:
            text = f"{doc.get('title', '')} {doc.get('description', '')}".lower()
            
            # Count keyword matches
            matches = sum(1 for kw in keywords if kw in text)
            
            # Combine with base relevance score
            base_score = self._calculate_relevance_score(doc)
            measure_score = base_score + (matches * 20)
            
            scored_docs.append({
                **doc,
                'measure_relevance_score': measure_score
            })
        
        # Sort and return top N
        scored_docs.sort(key=lambda x: x['measure_relevance_score'], reverse=True)
        return scored_docs[:top_n]
    
    def get_measure_category(self, measure_id: str) -> str:
        """Map measure ID to category for targeted filtering"""
        
        # Governance measures (M01-M02)
        if measure_id in ['M01', 'M02']:
            return 'governance'
        
        # Strategy measures (M03-M05)
        if measure_id in ['M03', 'M04', 'M05']:
            return 'strategy'
        
        # Risk Management measures (M06-M09)
        if measure_id in ['M06', 'M07', 'M08', 'M09']:
            return 'risk_management'
        
        # Metrics & Targets (M10-M17)
        if measure_id in ['M10', 'M11', 'M12', 'M13', 'M14', 'M15', 'M16', 'M17']:
            return 'metrics_targets'
        
        # Physical Risk - Acute (M18-M26)
        if measure_id in ['M18', 'M19', 'M20', 'M21', 'M22', 'M23', 'M24', 'M25', 'M26']:
            return 'physical_acute'
        
        # Physical Risk - Chronic (M27-M44)
        return 'physical_chronic'
