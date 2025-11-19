"""
Web Search Module - SerpAPI Integration
Uses Google search via SerpAPI for comprehensive, rate-limit-free searching
"""
import os
import logging
from typing import List, Dict
from serpapi import GoogleSearch

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


def search_company_climate_info(company_name: str, max_results: int = 25) -> List[Dict]:
    """
    Search for company-specific climate risk information using SerpAPI (Google)
    
    Args:
        company_name: Name of the company
        max_results: Maximum number of results to return (default 25)
    
    Returns:
        List of search results with title, url, snippet
    """
    serpapi_key = os.getenv('SERPAPI_KEY')
    if not serpapi_key:
        logger.error("SERPAPI_KEY not found in environment")
        return []
    
    logger.info(f"Searching for climate info: {company_name}")
    
    # Define search queries targeting physical climate risks
    queries = [
        f'"{company_name}" climate risk disclosure',
        f'"{company_name}" sustainability report climate',
        f'"{company_name}" ESG climate physical risk',
        f'"{company_name}" TCFD climate report',
        f'"{company_name}" CDP climate response',
        f'"{company_name}" 10-K climate risk',
        f'"{company_name}" climate adaptation resilience',
        f'"{company_name}" physical climate hazards'
    ]
    
    all_results = []
    seen_urls = set()
    
    for query in queries:
        try:
            # SerpAPI Google Search
            search = GoogleSearch({
                "q": query,
                "api_key": serpapi_key,
                "num": 10,  # Get 10 results per query
                "gl": "us",  # Geographic location
                "hl": "en"   # Language
            })
            
            results = search.get_dict()
            organic_results = results.get("organic_results", [])
            
            for result in organic_results:
                url = result.get("link", "")
                title = result.get("title", "")
                snippet = result.get("snippet", "")
                
                # Skip if already seen
                if url in seen_urls:
                    continue
                
                # Filter out generic domains
                if any(domain in url.lower() for domain in GENERIC_DOMAINS):
                    logger.debug(f"Filtered generic domain: {url}")
                    continue
                
                # Filter out generic content
                if any(keyword in title.lower() or keyword in snippet.lower() 
                       for keyword in GENERIC_KEYWORDS):
                    logger.debug(f"Filtered generic content: {title}")
                    continue
                
                # Check if company name appears in title or snippet
                if company_name.lower() not in (title + snippet).lower():
                    logger.debug(f"Company name not in result: {title}")
                    continue
                
                all_results.append({
                    'title': title,
                    'url': url,
                    'snippet': snippet,
                    'query': query
                })
                seen_urls.add(url)
                
                # Stop if we have enough results
                if len(all_results) >= max_results:
                    break
            
            if len(all_results) >= max_results:
                break
                
        except Exception as e:
            logger.warning(f"Query failed: {query} - {str(e)}")
            continue
    
    logger.info(f"Found {len(all_results)} company-specific results for {company_name}")
    return all_results[:max_results]


def format_search_results(results: List[Dict]) -> str:
    """
    Format search results into a structured string for AI processing
    
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
