"""
Enhanced Document Extraction Module
Implements Priority 1-2 improvements:
- Domain inference from company names
- Direct sustainability URL attempts
- SEC EDGAR integration
- Multi-pass search strategy
- Year-range searching
"""

import requests
import re
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, quote
import time

logger = logging.getLogger(__name__)


class EnhancedDocumentExtractor:
    """Enhanced document extraction with multiple strategies"""
    
    def __init__(self, serpapi_key: str):
        self.serpapi_key = serpapi_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def infer_company_domain(self, company_name: str) -> Optional[str]:
        """Infer company domain from company name"""
        # Clean company name
        name = company_name.lower()
        
        # Remove common suffixes
        suffixes = [
            'corporation', 'corp', 'inc', 'incorporated', 'ltd', 'limited',
            'plc', 'llc', 'company', 'co', 'group', 'holdings', 'international',
            'intl', 'sa', 'ag', 'nv', 'se', 'gmbh', 'shs', 'sbvtg'
        ]
        
        for suffix in suffixes:
            name = re.sub(rf'\b{suffix}\.?\b', '', name, flags=re.IGNORECASE)
        
        # Remove special characters and extra spaces
        name = re.sub(r'[^\w\s]', '', name)
        name = name.strip()
        
        # Handle multi-word names
        if ' ' in name:
            # Try first word
            first_word = name.split()[0]
            if len(first_word) > 3:
                return f"{first_word}.com"
            
            # Try concatenated
            concatenated = name.replace(' ', '')
            if len(concatenated) <= 20:
                return f"{concatenated}.com"
        
        # Single word
        if len(name) > 2:
            return f"{name}.com"
        
        return None
    
    def get_sustainability_urls(self, company_name: str, domain: Optional[str] = None) -> List[str]:
        """Generate likely sustainability report URLs"""
        if not domain:
            domain = self.infer_company_domain(company_name)
        
        if not domain:
            return []
        
        # Remove .com to get base
        base = domain.replace('.com', '').replace('.org', '').replace('.net', '')
        
        # Common sustainability page patterns
        urls = []
        
        # Base domains to try
        domains_to_try = [
            f"https://www.{domain}",
            f"https://{domain}",
            f"https://{base}.com",
            f"https://www.{base}.com"
        ]
        
        # Common paths
        paths = [
            "/sustainability",
            "/esg",
            "/corporate-responsibility",
            "/climate",
            "/environment",
            "/investors/esg",
            "/about/sustainability",
            "/responsibility"
        ]
        
        # Common report file patterns (2022-2024)
        for year in [2024, 2023, 2022]:
            report_patterns = [
                f"/sustainability-report-{year}.pdf",
                f"/esg-report-{year}.pdf",
                f"/climate-report-{year}.pdf",
                f"/tcfd-report-{year}.pdf",
                f"/corporate-responsibility-report-{year}.pdf",
                f"/annual-report-{year}.pdf",
                f"/reports/sustainability-{year}.pdf",
                f"/reports/esg-{year}.pdf"
            ]
            
            # Combine domains with paths and reports
            for dom in domains_to_try[:2]:  # Limit to avoid too many requests
                for path in paths[:3]:  # Top 3 paths
                    urls.append(dom + path)
                for report in report_patterns[:4]:  # Top 4 report patterns
                    urls.append(dom + report)
        
        return urls[:20]  # Limit to 20 URLs
    
    def try_direct_url(self, url: str) -> Optional[Dict[str, str]]:
        """Try to access a URL directly and extract content"""
        try:
            # First check if URL exists (HEAD request)
            head_resp = self.session.head(url, timeout=5, allow_redirects=True)
            
            if head_resp.status_code == 200:
                logger.info(f"✓ Found direct URL: {url}")
                
                # Extract content using Jina AI
                content = self.extract_with_jina(url)
                
                if content and len(content) > 1000:
                    return {
                        'url': url,
                        'content': content,
                        'source': 'direct_url'
                    }
        
        except Exception as e:
            logger.debug(f"Direct URL failed {url}: {e}")
        
        return None
    
    def extract_with_jina(self, url: str) -> Optional[str]:
        """Extract content from URL using Jina AI Reader"""
        try:
            jina_url = f"https://r.jina.ai/{url}"
            
            response = self.session.get(
                jina_url,
                headers={'Accept': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                data = result.get('data', {})
                content = data.get('content', '')
                
                if content and len(content) > 500:
                    logger.info(f"✓ Jina extracted {len(content)} chars from {url}")
                    return content[:50000]  # Limit to 50k chars
            
        except Exception as e:
            logger.warning(f"Jina extraction failed for {url}: {e}")
        
        return None
    
    def search_sec_edgar(self, company_name: str) -> List[Dict[str, str]]:
        """Search SEC EDGAR for 10-K filings with climate risk sections"""
        documents = []
        
        try:
            # Search for company in SEC
            search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
            params = {
                'action': 'getcompany',
                'company': company_name,
                'type': '10-K',
                'dateb': '',
                'owner': 'exclude',
                'count': 3,
                'search_text': ''
            }
            
            # Note: This is a simplified approach
            # Full implementation would parse SEC responses and extract climate sections
            # For now, we'll use SerpAPI to find SEC filings
            
            query = f"{company_name} 10-K climate risk site:sec.gov"
            results = self.serpapi_search(query, num_results=3)
            
            for result in results:
                if 'sec.gov' in result.get('link', ''):
                    content = self.extract_with_jina(result['link'])
                    if content:
                        documents.append({
                            'url': result['link'],
                            'content': content,
                            'source': 'sec_edgar'
                        })
        
        except Exception as e:
            logger.warning(f"SEC EDGAR search failed for {company_name}: {e}")
        
        return documents
    
    def serpapi_search(self, query: str, num_results: int = 10) -> List[Dict]:
        """Search using SerpAPI"""
        try:
            url = "https://serpapi.com/search.json"
            params = {
                'q': query,
                'api_key': self.serpapi_key,
                'num': num_results
            }
            
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('organic_results', [])
        
        except Exception as e:
            logger.warning(f"SerpAPI search failed: {e}")
        
        return []
    
    def multi_pass_search(self, company_name: str, measure_category: str) -> List[Dict]:
        """Multi-pass search with targeted queries"""
        all_results = []
        
        # Define category-specific queries
        category_queries = {
            'governance': [
                f'"{company_name}" board oversight climate risk',
                f'"{company_name}" climate governance TCFD',
                f'"{company_name}" board climate committee'
            ],
            'risk_assessment': [
                f'"{company_name}" physical climate risk assessment',
                f'"{company_name}" scenario analysis RCP',
                f'"{company_name}" vulnerability assessment facilities',
                f'"{company_name}" climate risk financial impact'
            ],
            'adaptation': [
                f'"{company_name}" climate adaptation infrastructure',
                f'"{company_name}" resilience measures',
                f'"{company_name}" climate risk mitigation operations',
                f'"{company_name}" supply chain climate resilience'
            ],
            'kpis': [
                f'"{company_name}" climate resilience KPI',
                f'"{company_name}" adaptation metrics',
                f'"{company_name}" physical risk indicators'
            ]
        }
        
        queries = category_queries.get(measure_category, [
            f'"{company_name}" climate risk',
            f'"{company_name}" sustainability report'
        ])
        
        for query in queries[:3]:  # Limit to 3 queries per category
            results = self.serpapi_search(query, num_results=5)
            all_results.extend(results)
            time.sleep(0.5)  # Rate limiting
        
        return all_results
    
    def extract_documents_for_company(
        self,
        company_name: str,
        max_documents: int = 5
    ) -> List[Dict[str, str]]:
        """
        Extract documents using multiple strategies
        
        Returns list of dicts with keys: url, content, source
        """
        documents = []
        
        logger.info(f"Starting enhanced extraction for {company_name}")
        
        # Strategy 1: Try direct sustainability URLs
        logger.info(f"Strategy 1: Trying direct sustainability URLs...")
        sustainability_urls = self.get_sustainability_urls(company_name)
        
        for url in sustainability_urls[:10]:  # Try first 10
            doc = self.try_direct_url(url)
            if doc:
                documents.append(doc)
                if len(documents) >= max_documents:
                    return documents
            time.sleep(0.3)  # Rate limiting
        
        # Strategy 2: SEC EDGAR (for US companies)
        logger.info(f"Strategy 2: Searching SEC EDGAR...")
        sec_docs = self.search_sec_edgar(company_name)
        documents.extend(sec_docs)
        
        if len(documents) >= max_documents:
            return documents[:max_documents]
        
        # Strategy 3: Enhanced web search
        logger.info(f"Strategy 3: Enhanced web search...")
        
        # Year-range search for sustainability reports
        for year in [2024, 2023, 2022]:
            if len(documents) >= max_documents:
                break
            
            query = f'"{company_name}" sustainability report {year} filetype:pdf'
            results = self.serpapi_search(query, num_results=3)
            
            for result in results:
                if len(documents) >= max_documents:
                    break
                
                url = result.get('link', '')
                if url and ('.pdf' in url.lower() or 'sustainability' in url.lower()):
                    content = self.extract_with_jina(url)
                    if content and len(content) > 1000:
                        documents.append({
                            'url': url,
                            'content': content,
                            'source': 'web_search'
                        })
            
            time.sleep(0.5)
        
        # Strategy 4: CDP search
        if len(documents) < max_documents:
            logger.info(f"Strategy 4: CDP search...")
            cdp_query = f'"{company_name}" CDP climate change response'
            cdp_results = self.serpapi_search(cdp_query, num_results=3)
            
            for result in cdp_results:
                if len(documents) >= max_documents:
                    break
                
                url = result.get('link', '')
                if 'cdp.net' in url or 'disclosure' in url.lower():
                    content = self.extract_with_jina(url)
                    if content and len(content) > 1000:
                        documents.append({
                            'url': url,
                            'content': content,
                            'source': 'cdp'
                        })
        
        logger.info(f"Extracted {len(documents)} documents for {company_name}")
        return documents[:max_documents]


def extract_documents_for_company(company_name: str, serpapi_key: str) -> List[Dict[str, str]]:
    """Wrapper function for compatibility"""
    extractor = EnhancedDocumentExtractor(serpapi_key)
    return extractor.extract_documents_for_company(company_name)
