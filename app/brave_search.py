"""
Brave Search API Integration with Adaptive Exhaustive Search

Replaces SerpAPI with Brave Search API for cost-effective, comprehensive document discovery.
Implements adaptive search that continues until document exhaustion is reached.
Uses ISIN to discover company name variations for more accurate results.
"""

import os
import logging
import requests
import time
import re
from typing import List, Dict, Set, Optional

logger = logging.getLogger(__name__)

# Generic domains to filter out
GENERIC_DOMAINS = {
    'ipcc.ch', 'unfccc.int', 'epa.gov', 'nasa.gov', 'noaa.gov',
    'wikipedia.org', 'britannica.com', 'investopedia.com',
    'worldbank.org', 'imf.org', 'oecd.org', 'un.org',
    'climate.gov', 'carbonbrief.org', 'climatecentral.org'
}

# Generic keywords that indicate non-company-specific content
GENERIC_KEYWORDS = {
    'what is climate change', 'climate change explained',
    'introduction to', 'overview of', 'guide to',
    'climate science', 'global warming basics'
}


class AdaptiveDocumentSearch:
    """
    Adaptive exhaustive search that continues until no new documents are found.
    Uses ISIN to discover company name variations for accurate filtering.
    """
    
    def __init__(self, brave_api_key: str = None, max_iterations: int = 10, max_documents: int = 150):
        self.brave_api_key = brave_api_key or os.getenv('BRAVE_API_KEY')
        if not self.brave_api_key:
            raise ValueError("BRAVE_API_KEY not provided and not found in environment")
            
        self.max_iterations = max_iterations
        self.max_documents = max_documents
        self.search_count = 0
        self.company_name_variations = set()
        
    def search(self, company_name: str, isin: str = None, verbose: bool = True) -> List[Dict]:
        """
        Perform adaptive exhaustive search for a company.
        
        Args:
            company_name: Name of the company
            isin: Optional ISIN code (highly recommended for accuracy)
            verbose: Whether to log progress
            
        Returns:
            List of unique document dictionaries with url, title, description
        """
        all_documents = {}  # URL -> metadata
        iteration = 0
        consecutive_no_new_docs = 0
        
        # Discover company name variations using ISIN
        if isin:
            self._discover_name_variations(company_name, isin)
        else:
            # Fallback: extract core name manually
            self.company_name_variations = self._extract_core_names(company_name)
        
        logger.info(f"Starting adaptive search for {company_name} (variations: {self.company_name_variations})")
        
        while iteration < self.max_iterations and len(all_documents) < self.max_documents:
            iteration += 1
            
            # Generate queries for this iteration
            queries = self._generate_queries(company_name, isin, iteration)
            
            # Search with these queries
            new_docs = self._search_iteration(queries, company_name, isin)
            
            # Count new documents
            new_urls = set(new_docs.keys())
            previously_unseen = new_urls - set(all_documents.keys())
            
            logger.info(f"Iteration {iteration}: Found {len(new_docs)} docs, "
                       f"{len(previously_unseen)} new (total: {len(all_documents) + len(previously_unseen)})")
            
            # Add new documents
            for url in previously_unseen:
                all_documents[url] = new_docs[url]
            
            # Check termination condition
            if len(previously_unseen) == 0:
                consecutive_no_new_docs += 1
                
                if consecutive_no_new_docs >= 2:
                    # No new docs for 2 iterations - we're exhausted
                    logger.info(f"Search exhausted after {iteration} iterations")
                    break
            else:
                # Found new docs, reset counter
                consecutive_no_new_docs = 0
        
        if iteration >= self.max_iterations:
            logger.warning(f"Reached max iterations ({self.max_iterations})")
        
        if len(all_documents) >= self.max_documents:
            logger.warning(f"Reached max documents ({self.max_documents})")
        
        logger.info(f"Total unique documents: {len(all_documents)}")
        
        return list(all_documents.values())
    
    def _discover_name_variations(self, company_name: str, isin: str):
        """
        Use ISIN to discover company name variations from authoritative sources.
        """
        logger.info(f"Discovering name variations for {company_name} using ISIN {isin}")
        
        # Start with provided name
        self.company_name_variations = self._extract_core_names(company_name)
        
        # Search for ISIN to find official company names
        try:
            results = self._brave_search(f'{isin} company name', count=10)
            
            for result in results[:5]:  # Check first 5 results
                title = result.get('title', '')
                description = result.get('description', '')
                
                # Extract potential company names from title/description
                # Look for patterns like "Company Name (ISIN: ...)" or "ISIN - Company Name"
                combined = title + ' ' + description
                
                # Extract names near ISIN
                isin_pattern = rf'([A-Z][a-zA-Z\s&\.]+)\s*[\(\-]?\s*{isin}'
                matches = re.findall(isin_pattern, combined)
                
                for match in matches:
                    variations = self._extract_core_names(match.strip())
                    self.company_name_variations.update(variations)
            
            logger.info(f"Discovered variations: {self.company_name_variations}")
            
        except Exception as e:
            logger.warning(f"Failed to discover name variations: {e}")
            # Fallback to manual extraction
            self.company_name_variations = self._extract_core_names(company_name)
    
    def _extract_core_names(self, company_name: str) -> Set[str]:
        """
        Extract core company name and common variations.
        
        Examples:
        - "Cisco Systems Inc." -> {"cisco", "cisco systems"}
        - "Volkswagen AG" -> {"volkswagen", "vw"}
        - "Apple Inc." -> {"apple"}
        """
        variations = set()
        
        # Remove common suffixes
        suffixes = ['inc', 'incorporated', 'corp', 'corporation', 'ltd', 'limited', 
                   'ag', 'plc', 'sa', 'nv', 'gmbh', 'co', 'company', 'group']
        
        name_lower = company_name.lower()
        
        # Remove punctuation
        name_clean = re.sub(r'[^\w\s]', ' ', name_lower)
        
        # Split into words
        words = name_clean.split()
        
        # Filter out suffixes
        core_words = [w for w in words if w not in suffixes and len(w) > 2]
        
        if not core_words:
            # Fallback to original name
            variations.add(name_lower.strip())
            return variations
        
        # Add first significant word (e.g., "cisco" from "Cisco Systems")
        variations.add(core_words[0])
        
        # Add first two words if available (e.g., "cisco systems")
        if len(core_words) >= 2:
            variations.add(f"{core_words[0]} {core_words[1]}")
        
        # Add full core name
        variations.add(' '.join(core_words))
        
        # Add common abbreviations for multi-word names
        if len(core_words) >= 2:
            # e.g., "Volkswagen" -> "VW"
            abbrev = ''.join([w[0] for w in core_words[:3]])  # First 3 words
            if len(abbrev) >= 2:
                variations.add(abbrev.lower())
        
        return variations
    
    def _generate_queries(self, company_name: str, isin: str, iteration_num: int) -> List[str]:
        """
        Generate diverse queries for each iteration.
        """
        # Extract company domain for site-specific searches
        domain = self._extract_domain(company_name)
        
        # Use primary name variation for queries
        primary_name = list(self.company_name_variations)[0] if self.company_name_variations else company_name
        
        # Define query strategies by iteration
        all_queries = [
            # Iteration 1: Core climate risk with ISIN
            [
                f'{isin} climate physical risk' if isin else f'"{company_name}" climate physical risk',
                f'{isin} TCFD disclosure' if isin else f'"{company_name}" TCFD physical risk',
                f'"{primary_name}" climate adaptation resilience'
            ],
            # Iteration 2: Specific hazards
            [
                f'"{primary_name}" flood risk extreme weather',
                f'"{primary_name}" drought water stress',
                f'"{primary_name}" extreme heat climate'
            ],
            # Iteration 3: Site-specific
            [
                f'site:{domain} climate risk' if domain else f'"{primary_name}" climate risk report',
                f'site:{domain} sustainability report' if domain else f'"{primary_name}" sustainability disclosure',
                f'site:{domain} ESG climate' if domain else f'"{primary_name}" ESG climate disclosure'
            ],
            # Iteration 4: Regulatory & frameworks
            [
                f'"{primary_name}" CDP climate disclosure',
                f'"{primary_name}" EU Taxonomy physical risk',
                f'{isin} climate risk assessment' if isin else f'"{primary_name}" climate scenario analysis'
            ],
            # Iteration 5: Business impact
            [
                f'"{primary_name}" business continuity climate',
                f'"{primary_name}" supply chain climate risk',
                f'"{primary_name}" asset resilience climate'
            ],
            # Iteration 6: Reporting
            [
                f'"{primary_name}" annual report climate risk',
                f'"{primary_name}" 10-K climate physical risk',
                f'"{primary_name}" integrated report climate'
            ],
            # Iteration 7: Geographic & sector
            [
                f'"{primary_name}" facilities climate vulnerability',
                f'"{primary_name}" operations climate exposure',
                f'"{primary_name}" infrastructure climate risk'
            ],
            # Iteration 8: Investor & analyst
            [
                f'"{primary_name}" investor presentation climate',
                f'"{primary_name}" analyst report climate risk',
                f'"{primary_name}" ESG rating climate'
            ],
            # Iteration 9: Deep dive with variations
            [
                f'"{var}" climate risk' for var in list(self.company_name_variations)[:3]
            ],
            # Iteration 10: Final sweep
            [
                f'{isin} sustainability' if isin else f'"{primary_name}" sustainability',
                f'{isin} environmental disclosure' if isin else f'"{primary_name}" environmental disclosure',
                f'"{primary_name}" climate change strategy'
            ]
        ]
        
        # Get queries for this iteration (cycle if beyond defined iterations)
        iteration_idx = (iteration_num - 1) % len(all_queries)
        queries = all_queries[iteration_idx]
        
        # Flatten list (in case of nested lists)
        flat_queries = []
        for q in queries:
            if isinstance(q, list):
                flat_queries.extend(q)
            else:
                flat_queries.append(q)
        
        return flat_queries
    
    def _search_iteration(self, queries: List[str], company_name: str, isin: str = None) -> Dict[str, Dict]:
        """
        Execute searches for all queries in this iteration.
        """
        all_results = {}
        
        for query in queries:
            try:
                results = self._brave_search(query, count=20)
                
                for result in results:
                    url = result['url']
                    title = result.get('title', '')
                    snippet = result.get('description', '')
                    
                    # Filter out irrelevant results
                    if not self._should_filter(url, title, snippet, company_name, isin):
                        if url not in all_results:
                            all_results[url] = result
                
                self.search_count += 1
                time.sleep(0.1)  # Small delay to be respectful
                
            except Exception as e:
                logger.warning(f"Search failed for '{query[:50]}...': {e}")
        
        return all_results
    
    def _brave_search(self, query: str, count: int = 20) -> List[Dict]:
        """
        Execute Brave search API call.
        """
        url = "https://api.search.brave.com/res/v1/web/search"
        
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.brave_api_key
        }
        
        params = {
            "q": query,
            "count": count,
            "search_lang": "en"
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        results = []
        
        if 'web' in data and 'results' in data['web']:
            for item in data['web']['results']:
                results.append({
                    'url': item['url'],
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'query': query
                })
        
        return results
    
    def _should_filter(self, url: str, title: str, snippet: str, company_name: str, isin: str = None) -> bool:
        """
        Determine if a result should be filtered out.
        Uses discovered name variations and ISIN for accurate matching.
        """
        # Filter out generic domains
        if any(domain in url.lower() for domain in GENERIC_DOMAINS):
            logger.debug(f"Filtered generic domain: {url}")
            return True
        
        # Filter out generic content
        if any(keyword in title.lower() or keyword in snippet.lower() 
               for keyword in GENERIC_KEYWORDS):
            logger.debug(f"Filtered generic content: {title}")
            return True
        
        combined_text = (title + ' ' + snippet).lower()
        
        # Priority 1: Check for ISIN (most reliable)
        if isin and isin.lower() in combined_text:
            logger.debug(f"Accepted (ISIN match): {title}")
            return False
        
        # Priority 2: Check for any discovered name variation
        if self.company_name_variations:
            for variation in self.company_name_variations:
                if variation in combined_text:
                    # Additional check: ensure climate/sustainability context
                    climate_keywords = ['climate', 'sustainability', 'esg', 'environmental', 
                                       'tcfd', 'cdp', 'carbon', 'emissions', 'risk']
                    if any(kw in combined_text for kw in climate_keywords):
                        logger.debug(f"Accepted (variation '{variation}' + climate context): {title}")
                        return False
            
            # Name variation found but no climate context
            logger.debug(f"Filtered (name found but no climate context): {title}")
            return True
        
        # No match found
        logger.debug(f"Filtered (no company match): {title}")
        return True
    
    def _extract_domain(self, company_name: str) -> str:
        """
        Extract likely company domain from name.
        """
        # Simple heuristic: take first word, lowercase, add .com
        words = company_name.split()
        if words:
            core = words[0].lower().replace('.', '').replace(',', '')
            return f"{core}.com"
        return ""


def search_documents(company_name: str, isin: str = None, max_iterations: int = 10) -> List[Dict]:
    """
    Convenience function to search for company climate documents.
    
    Args:
        company_name: Name of the company
        isin: Optional ISIN code (recommended)
        max_iterations: Maximum search iterations
        
    Returns:
        List of document dictionaries
    """
    searcher = AdaptiveDocumentSearch(max_iterations=max_iterations)
    return searcher.search(company_name, isin)
