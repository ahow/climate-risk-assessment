"""
Sustainability Portal Integration
Searches sustainability report databases and portals for company reports
"""
import logging
import requests
from typing import List, Dict, Optional
from serpapi import GoogleSearch
import os

logger = logging.getLogger(__name__)


class SustainabilityPortalSearch:
    """Search sustainability report portals and databases"""
    
    def __init__(self):
        self.serpapi_key = os.getenv('SERPAPI_KEY')
    
    def search_all_portals(self, company_name: str) -> List[Dict]:
        """
        Search all sustainability portals for company reports
        
        Args:
            company_name: Name of the company
            
        Returns:
            List of report URLs with metadata
        """
        all_reports = []
        
        # Search multiple portals
        all_reports.extend(self._search_cdp(company_name))
        all_reports.extend(self._search_gri_database(company_name))
        all_reports.extend(self._search_company_website(company_name))
        all_reports.extend(self._search_sec_edgar(company_name))
        
        logger.info(f"Found {len(all_reports)} reports from sustainability portals")
        return all_reports
    
    def _search_cdp(self, company_name: str) -> List[Dict]:
        """Search CDP (Carbon Disclosure Project) database"""
        reports = []
        
        if not self.serpapi_key:
            return reports
        
        try:
            # Search CDP website for company responses
            search = GoogleSearch({
                "q": f'site:cdp.net "{company_name}" climate change response',
                "api_key": self.serpapi_key,
                "num": 5
            })
            
            results = search.get_dict()
            
            for result in results.get("organic_results", []):
                url = result.get("link", "")
                if 'cdp.net' in url.lower():
                    reports.append({
                        'url': url,
                        'title': result.get("title", "CDP Climate Response"),
                        'source': 'CDP',
                        'type': 'climate_disclosure'
                    })
            
            logger.info(f"Found {len(reports)} CDP reports for {company_name}")
        except Exception as e:
            logger.warning(f"CDP search failed: {e}")
        
        return reports
    
    def _search_gri_database(self, company_name: str) -> List[Dict]:
        """Search GRI (Global Reporting Initiative) database"""
        reports = []
        
        if not self.serpapi_key:
            return reports
        
        try:
            # Search GRI database
            search = GoogleSearch({
                "q": f'site:database.globalreporting.org "{company_name}" sustainability report',
                "api_key": self.serpapi_key,
                "num": 5
            })
            
            results = search.get_dict()
            
            for result in results.get("organic_results", []):
                url = result.get("link", "")
                if 'globalreporting.org' in url.lower():
                    reports.append({
                        'url': url,
                        'title': result.get("title", "GRI Sustainability Report"),
                        'source': 'GRI',
                        'type': 'sustainability_report'
                    })
            
            logger.info(f"Found {len(reports)} GRI reports for {company_name}")
        except Exception as e:
            logger.warning(f"GRI search failed: {e}")
        
        return reports
    
    def _search_company_website(self, company_name: str) -> List[Dict]:
        """Search company website for sustainability/ESG reports"""
        reports = []
        
        if not self.serpapi_key:
            return reports
        
        try:
            # Common company website patterns
            company_domain = company_name.lower().replace(' ', '').replace('.', '')
            
            # Search for PDF reports on company website
            queries = [
                f'site:{company_domain}.com sustainability report filetype:pdf',
                f'site:{company_domain}.com ESG report filetype:pdf',
                f'site:{company_domain}.com climate report filetype:pdf',
                f'site:{company_domain}.com TCFD report filetype:pdf',
                f'site:{company_domain}.com annual report climate filetype:pdf'
            ]
            
            for query in queries:
                try:
                    search = GoogleSearch({
                        "q": query,
                        "api_key": self.serpapi_key,
                        "num": 3
                    })
                    
                    results = search.get_dict()
                    
                    for result in results.get("organic_results", []):
                        url = result.get("link", "")
                        if url.lower().endswith('.pdf'):
                            reports.append({
                                'url': url,
                                'title': result.get("title", "Company Report"),
                                'source': 'Company Website',
                                'type': 'pdf_report'
                            })
                except Exception as e:
                    logger.debug(f"Query failed: {query} - {e}")
                    continue
            
            logger.info(f"Found {len(reports)} company website reports")
        except Exception as e:
            logger.warning(f"Company website search failed: {e}")
        
        return reports
    
    def _search_sec_edgar(self, company_name: str) -> List[Dict]:
        """Search SEC EDGAR for 10-K filings (US companies)"""
        reports = []
        
        if not self.serpapi_key:
            return reports
        
        try:
            # Search SEC EDGAR for 10-K with climate mentions
            search = GoogleSearch({
                "q": f'site:sec.gov "{company_name}" 10-K climate risk',
                "api_key": self.serpapi_key,
                "num": 3
            })
            
            results = search.get_dict()
            
            for result in results.get("organic_results", []):
                url = result.get("link", "")
                if 'sec.gov' in url.lower() and '10-k' in result.get("title", "").lower():
                    reports.append({
                        'url': url,
                        'title': result.get("title", "SEC 10-K Filing"),
                        'source': 'SEC EDGAR',
                        'type': 'sec_filing'
                    })
            
            logger.info(f"Found {len(reports)} SEC filings for {company_name}")
        except Exception as e:
            logger.warning(f"SEC EDGAR search failed: {e}")
        
        return reports


def get_priority_documents(company_name: str) -> List[Dict]:
    """
    Get prioritized list of documents for a company
    
    Args:
        company_name: Name of the company
        
    Returns:
        List of document URLs prioritized by relevance
    """
    portal = SustainabilityPortalSearch()
    all_reports = portal.search_all_portals(company_name)
    
    # Prioritize by source quality
    priority_order = {
        'CDP': 1,
        'Company Website': 2,
        'GRI': 3,
        'SEC EDGAR': 4
    }
    
    # Sort by priority
    all_reports.sort(key=lambda x: priority_order.get(x.get('source', ''), 99))
    
    # Remove duplicates
    seen_urls = set()
    unique_reports = []
    for report in all_reports:
        url = report['url']
        if url not in seen_urls:
            unique_reports.append(report)
            seen_urls.add(url)
    
    logger.info(f"Returning {len(unique_reports)} prioritized documents")
    return unique_reports
