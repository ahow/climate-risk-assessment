"""
Brave Search API Integration with Adaptive Exhaustive Search

Replaces SerpAPI with Brave Search API for cost-effective, comprehensive document discovery.
Implements adaptive search that continues until document exhaustion is reached.
"""

import os
import logging
import requests
import time
from typing import List, Dict, Set

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
    """
    
    def __init__(self, brave_api_key: str = None, max_iterations: int = 10, max_documents: int = 150):
        self.brave_api_key = brave_api_key or os.getenv('BRAVE_API_KEY')
        if not self.brave_api_key:
            raise ValueError("BRAVE_API_KEY not provided and not found in environment")
            
        self.max_iterations = max_iterations
        self.max_documents = max_documents
        self.search_count = 0
        
    def search(self, company_name: str, isin: str = None, verbose: bool = True) -> List[Dict]:
        """
        Perform adaptive exhaustive search for a company.
        
        Args:
            company_name: Name of the company
            isin: Optional ISIN code
            verbose: Whether to log progress
            
        Returns:
            List of unique document dictionaries with url, title, description
        """
        all_documents = {}  # URL -> metadata
        iteration = 0
        consecutive_no_new_docs = 0
        
        logger.info(f"Starting adaptive search for {company_name}")
        
        while iteration < self.max_iterations and len(all_documents) < self.max_documents:
            iteration += 1
            
            # Generate queries for this iteration
            queries = self._generate_queries(company_name, isin, iteration)
            
            # Search with these queries
            new_docs = self._search_iteration(queries, company_name)
            
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
    
    def _generate_queries(self, company_name: str, isin: str, iteration_num: int) -> List[str]:
        """
        Generate diverse queries for each iteration.
        """
        # Extract company domain for site-specific searches
        domain = self._extract_domain(company_name)
        
        # Define query strategies by iteration
        all_queries = [
            # Iteration 1: Core climate risk
            [
                f'"{company_name}" climate physical risk',
                f'"{company_name}" TCFD physical risk',
                f'"{company_name}" climate adaptation resilience'
            ],
            # Iteration 2: Specific hazards
            [
                f'"{company_name}" flood risk extreme weather',
                f'"{company_name}" drought water stress',
                f'"{company_name}" extreme heat climate'
            ],
            # Iteration 3: Site-specific
            [
                f'site:{domain} climate risk' if domain else f'{company_name} climate risk report',
                f'site:{domain} sustainability report' if domain else f'{company_name} sustainability disclosure',
                f'site:{domain} ESG climate' if domain else f'{company_name} ESG climate disclosure'
            ],
            # Iteration 4: Regulatory & frameworks
            [
                f'"{company_name}" CDP climate disclosure',
                f'"{company_name}" EU Taxonomy physical risk',
                f'{isin} climate risk assessment' if isin else f'{company_name} climate scenario analysis'
            ],
            # Iteration 5: Business impact
            [
                f'"{company_name}" business continuity climate',
                f'"{company_name}" supply chain climate risk',
                f'"{company_name}" asset resilience climate'
            ],
            # Iteration 6: Reporting
            [
                f'"{company_name}" annual report climate risk',
                f'"{company_name}" 10-K climate physical risk',
                f'"{company_name}" integrated report climate'
            ],
            # Iteration 7+: Long-tail
            [
                f'"{company_name}" climate change impact assessment',
                f'"{company_name}" environmental risk management',
                f'"{company_name}" climate vulnerability'
            ]
        ]
        
        # Get queries for this iteration (cycle through if needed)
        idx = (iteration_num - 1) % len(all_queries)
        return all_queries[idx]
    
    def _search_iteration(self, queries: List[str], company_name: str) -> Dict[str, Dict]:
        """
        Execute multiple queries and return deduplicated, filtered results.
        """
        all_results = {}
        
        for query in queries:
            try:
                results = self._brave_search(query, count=20)
                
                for result in results:
                    url = result['url']
                    
                    # Apply filters
                    if self._should_filter(url, result.get('title', ''), 
                                          result.get('description', ''), company_name):
                        continue
                    
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
    
    def _should_filter(self, url: str, title: str, snippet: str, company_name: str) -> bool:
        """
        Determine if a result should be filtered out.
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
        
        # Check if company name appears in title or snippet
        # Use first significant word of company name for flexibility
        # e.g., "Cisco Systems" -> check for "cisco"
        company_keywords = [word.lower() for word in company_name.split() 
                           if len(word) > 3 and word.lower() not in {'inc', 'ltd', 'corp', 'corporation', 'company', 'group'}]
        
        if not company_keywords:
            # Fallback to full name if no significant keywords
            company_keywords = [company_name.lower()]
        
        combined_text = (title + snippet).lower()
        if not any(keyword in combined_text for keyword in company_keywords):
            logger.debug(f"Company keywords {company_keywords} not in result: {title}")
            return True
        
        return False
    
    def _extract_domain(self, company_name: str) -> str:
        """
        Extract likely domain from company name.
        """
        # Simple heuristic - convert company name to domain
        domain = company_name.lower()
        domain = domain.replace(' ', '')
        domain = domain.replace('inc', '').replace('ltd', '').replace('corp', '')
        domain = domain.replace('.', '').replace(',', '')
        
        # Common patterns (can be expanded)
        domain_map = {
            'cisco': 'cisco.com',
            'apple': 'apple.com',
            'microsoft': 'microsoft.com',
            'google': 'google.com',
            'amazon': 'amazon.com',
            'facebook': 'facebook.com',
            'meta': 'meta.com',
            'tesla': 'tesla.com',
            'nvidia': 'nvidia.com',
            'intel': 'intel.com',
            'walmart': 'walmart.com',
            'visa': 'visa.com'
        }
        
        for key, value in domain_map.items():
            if key in domain:
                return value
        
        # Return None if can't determine
        return None


def search_company_climate_info(company_name: str, isin: str = None, max_results: int = 150) -> List[Dict]:
    """
    Search for company-specific climate risk information using Brave Search with adaptive exhaustive search.
    
    This is the main entry point that replaces the SerpAPI search function.
    
    Args:
        company_name: Name of the company
        isin: Optional ISIN code
        max_results: Maximum number of results to return (default 150)
    
    Returns:
        List of search results with title, url, snippet (description)
    """
    try:
        searcher = AdaptiveDocumentSearch(max_documents=max_results)
        documents = searcher.search(company_name, isin, verbose=True)
        
        # Convert to format expected by downstream code
        results = []
        for doc in documents:
            results.append({
                'title': doc.get('title', ''),
                'url': doc.get('url', ''),
                'snippet': doc.get('description', ''),  # Map 'description' to 'snippet'
                'query': doc.get('query', '')
            })
        
        logger.info(f"Found {len(results)} company-specific results for {company_name}")
        return results
        
    except Exception as e:
        logger.error(f"Adaptive search failed for {company_name}: {e}")
        return []


def format_search_results(results: List[Dict]) -> str:
    """
    Format search results into a structured string for AI processing.
    
    Args:
        results: List of search result dictionaries
        
    Returns:
        Formatted string with numbered sources
    """
    if not results:
        return "No web search results found."
    
    formatted = "WEB SEARCH RESULTS:\n\n"
    
    for i, result in enumerate(results, 1):
        formatted += f"[Source {i}]\n"
        formatted += f"Title: {result.get('title', 'N/A')}\n"
        formatted += f"URL: {result.get('url', 'N/A')}\n"
        formatted += f"Content: {result.get('snippet', 'N/A')}\n"
        formatted += "\n"
    
    return formatted
