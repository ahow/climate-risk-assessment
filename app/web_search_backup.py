"""
Web Search Module for Climate Risk Assessment
Uses DuckDuckGo with strict quality filtering for company-specific content
"""
import logging
from typing import List, Dict
from duckduckgo_search import DDGS

logger = logging.getLogger(__name__)

# Generic climate resources to filter out
GENERIC_DOMAINS = {
    'ipcc.ch', 'unfccc.int', 'climate.gov', 'noaa.gov',
    'nasa.gov', 'epa.gov', 'worldbank.org', 'un.org',
    'wikipedia.org', 'britannica.com', 'investopedia.com'
}

# Keywords that indicate generic content
GENERIC_KEYWORDS = [
    'what is climate change', 'climate change basics', 'introduction to',
    'climate science', 'global warming explained', 'climate 101'
]

def is_company_specific(result: Dict, company_name: str) -> bool:
    """
    Check if search result is company-specific
    
    Args:
        result: Search result dict with 'title', 'body', 'href'
        company_name: Company name to check for
        
    Returns:
        True if result appears to be company-specific
    """
    title = result.get('title', '').lower()
    body = result.get('body', '').lower()
    url = result.get('href', '').lower()
    company_lower = company_name.lower()
    
    # Check if company name appears in title or body
    company_mentioned = company_lower in title or company_lower in body or company_lower in url
    
    # Check for generic domains
    is_generic_domain = any(domain in url for domain in GENERIC_DOMAINS)
    
    # Check for generic keywords
    has_generic_keywords = any(keyword in title or keyword in body for keyword in GENERIC_KEYWORDS)
    
    # Result is company-specific if:
    # - Company is mentioned AND
    # - Not from generic domain AND
    # - Doesn't contain generic keywords
    return company_mentioned and not is_generic_domain and not has_generic_keywords

def search_company_climate_info(company_name: str, max_results: int = 10) -> List[Dict]:
    """
    Search for company-specific climate risk information
    
    Args:
        company_name: Name of the company
        max_results: Maximum number of results to return
        
    Returns:
        List of filtered search results
    """
    logger.info(f"Searching for climate info: {company_name}")
    
    # Construct search query focused on company climate disclosures
    queries = [
        f'"{company_name}" climate risk disclosure',
        f'"{company_name}" sustainability report climate',
        f'"{company_name}" ESG climate physical risk',
        f'"{company_name}" TCFD climate report'
    ]
    
    all_results = []
    seen_urls = set()
    
    try:
        with DDGS() as ddgs:
            for query in queries:
                logger.debug(f"Query: {query}")
                
                try:
                    results = list(ddgs.text(query, max_results=5))
                    
                    for result in results:
                        url = result.get('href', '')
                        
                        # Skip duplicates
                        if url in seen_urls:
                            continue
                        
                        # Apply quality filtering
                        if is_company_specific(result, company_name):
                            all_results.append({
                                'title': result.get('title', ''),
                                'url': url,
                                'snippet': result.get('body', ''),
                                'source': 'duckduckgo'
                            })
                            seen_urls.add(url)
                            logger.debug(f"✓ Added: {result.get('title', '')[:50]}")
                        else:
                            logger.debug(f"✗ Filtered: {result.get('title', '')[:50]}")
                        
                        # Stop if we have enough results
                        if len(all_results) >= max_results:
                            break
                    
                except Exception as e:
                    logger.warning(f"Query failed: {query} - {e}")
                    continue
                
                # Stop if we have enough results
                if len(all_results) >= max_results:
                    break
        
        logger.info(f"Found {len(all_results)} company-specific results for {company_name}")
        return all_results[:max_results]
        
    except Exception as e:
        logger.error(f"Web search failed for {company_name}: {e}")
        return []

def format_search_results(results: List[Dict]) -> str:
    """
    Format search results for inclusion in AI prompt
    
    Args:
        results: List of search result dicts
        
    Returns:
        Formatted string of search results
    """
    if not results:
        return "No company-specific web search results found."
    
    formatted = "## Web Search Results (Company-Specific)\n\n"
    
    for i, result in enumerate(results, 1):
        formatted += f"### Result {i}: {result['title']}\n"
        formatted += f"**URL:** {result['url']}\n"
        formatted += f"**Snippet:** {result['snippet']}\n\n"
    
    return formatted
