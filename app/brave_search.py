"""
Adaptive Exhaustive Document Search for Climate Risk Assessment

Searches until no new documents are found, then confirms with one more iteration.
Integrated version for Heroku deployment.
"""

import requests
import time
from typing import List, Dict, Set
import os


class AdaptiveDocumentSearch:
    """
    Adaptive search that continues until document exhaustion.
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
        
        Returns:
            List of unique document URLs with metadata
        """
        all_documents = {}  # URL -> metadata
        iteration = 0
        consecutive_no_new_docs = 0
        
        if verbose:
            print(f"🔍 Starting adaptive search for {company_name}")
        
        while iteration < self.max_iterations and len(all_documents) < self.max_documents:
            iteration += 1
            
            # Generate queries for this iteration
            queries = self._generate_queries(company_name, isin, iteration)
            
            # Search with these queries
            new_docs = self._search_iteration(queries, verbose=verbose)
            
            # Count new documents
            new_urls = set(new_docs.keys())
            previously_unseen = new_urls - set(all_documents.keys())
            
            if verbose:
                print(f"  Iteration {iteration}: Found {len(new_docs)} docs, "
                      f"{len(previously_unseen)} new (total: {len(all_documents) + len(previously_unseen)})")
            
            # Add new documents
            for url in previously_unseen:
                all_documents[url] = new_docs[url]
            
            # Check termination condition
            if len(previously_unseen) == 0:
                consecutive_no_new_docs += 1
                
                if consecutive_no_new_docs >= 2:
                    # No new docs for 2 iterations - we're exhausted
                    if verbose:
                        print(f"  ✅ Search exhausted after {iteration} iterations")
                    break
            else:
                # Found new docs, reset counter
                consecutive_no_new_docs = 0
        
        if verbose:
            if iteration >= self.max_iterations:
                print(f"  ⚠️  Reached max iterations ({self.max_iterations})")
            
            if len(all_documents) >= self.max_documents:
                print(f"  ⚠️  Reached max documents ({self.max_documents})")
            
            print(f"  📚 Total unique documents: {len(all_documents)}")
        
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
    
    def _search_iteration(self, queries: List[str], verbose: bool = True) -> Dict[str, Dict]:
        """
        Execute multiple queries and return deduplicated results.
        """
        all_results = {}
        
        for query in queries:
            try:
                results = self._brave_search(query, count=20)
                
                for result in results:
                    url = result['url']
                    if url not in all_results:
                        all_results[url] = result
                
                self.search_count += 1
                time.sleep(0.1)  # Small delay to be respectful
                
            except Exception as e:
                if verbose:
                    print(f"    ⚠️  Search failed for '{query[:50]}...': {e}")
        
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
                # Skip malformed results without URL
                if 'url' not in item:
                    continue
                    
                results.append({
                    'url': item['url'],
                    'title': item.get('title', ''),
                    'description': item.get('description', ''),
                    'query': query
                })
        
        return results
    
    def _extract_domain(self, company_name: str) -> str:
        """
        Extract likely domain from company name.
        """
        # Simple heuristic - convert company name to domain
        domain = company_name.lower()
        domain = domain.replace(' ', '')
        domain = domain.replace('inc', '').replace('ltd', '').replace('corp', '')
        domain = domain.replace('.', '').replace(',', '')
        
        # Common patterns
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
            'intel': 'intel.com'
        }
        
        for key, value in domain_map.items():
            if key in domain:
                return value
        
        # Return None if can't determine
        return None


def search_company_documents(company_name: str, isin: str = None, verbose: bool = False) -> List[str]:
    """
    Convenience function to search for company documents.
    
    Returns:
        List of document URLs
    """
    searcher = AdaptiveDocumentSearch()
    documents = searcher.search(company_name, isin, verbose=verbose)
    return [doc['url'] for doc in documents]


# Backward compatibility wrappers
def search_company_climate_info(company_name: str, isin: str = None, max_results: int = 150) -> List[Dict]:
    searcher = AdaptiveDocumentSearch(max_documents=max_results)
    return searcher.search(company_name, isin, verbose=False)

def format_search_results(results: List[Dict]) -> str:
    if not results:
        return "No relevant documents found."
    formatted = []
    for i, doc in enumerate(results, 1):
        formatted.append(f"{i}. {doc.get('title', 'No title')}\n   URL: {doc.get('url', '')}\n   {doc.get('description', '')}\n")
    return "\n".join(formatted)
