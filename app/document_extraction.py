"""
Document Extraction Module - Comprehensive Climate Risk Document Retrieval
Retrieves and extracts text from sustainability reports, PDFs, and web pages
"""
import os
import logging
import requests
from typing import List, Dict, Optional
from urllib.parse import urlparse
import re
import time

logger = logging.getLogger(__name__)

# PDF extraction
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("PyPDF2 not available - PDF extraction disabled")

# HTML parsing
from bs4 import BeautifulSoup


class DocumentExtractor:
    """Extract text from various document sources for climate risk assessment"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.max_doc_size = 10 * 1024 * 1024  # 10MB max
        self.timeout = 30
    
    def extract_from_url(self, url: str) -> Optional[Dict]:
        """
        Extract text content from a URL (PDF or HTML)
        
        Args:
            url: URL to extract from
            
        Returns:
            Dict with 'text', 'url', 'type', 'title' or None if failed
        """
        try:
            # Determine content type
            if url.lower().endswith('.pdf'):
                return self._extract_pdf(url)
            else:
                return self._extract_html(url)
        except Exception as e:
            logger.error(f"Failed to extract from {url}: {e}")
            return None
    
    def _extract_pdf(self, url: str) -> Optional[Dict]:
        """Extract text from PDF URL"""
        if not PDF_AVAILABLE:
            logger.warning(f"PDF extraction not available for {url}")
            return None
        
        try:
            logger.info(f"Downloading PDF: {url}")
            response = self.session.get(url, timeout=self.timeout, stream=True)
            
            # Check size
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > self.max_doc_size:
                logger.warning(f"PDF too large: {content_length} bytes")
                return None
            
            # Save temporarily
            temp_path = f"/tmp/climate_doc_{int(time.time())}.pdf"
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Extract text
            text = ""
            with open(temp_path, 'rb') as f:
                pdf_reader = PyPDF2.PdfReader(f)
                num_pages = len(pdf_reader.pages)
                
                # Limit to first 50 pages to avoid excessive processing
                max_pages = min(num_pages, 50)
                logger.info(f"Extracting text from {max_pages} pages")
                
                for page_num in range(max_pages):
                    try:
                        page = pdf_reader.pages[page_num]
                        text += page.extract_text() + "\n"
                    except Exception as e:
                        logger.warning(f"Failed to extract page {page_num}: {e}")
                        continue
            
            # Clean up
            os.remove(temp_path)
            
            # Clean text
            text = self._clean_text(text)
            
            if len(text) < 100:
                logger.warning(f"Extracted text too short: {len(text)} chars")
                return None
            
            logger.info(f"Extracted {len(text)} chars from PDF")
            
            return {
                'text': text[:50000],  # Limit to 50k chars
                'url': url,
                'type': 'pdf',
                'title': self._extract_title_from_url(url)
            }
            
        except Exception as e:
            logger.error(f"PDF extraction failed for {url}: {e}")
            return None
    
    def _extract_html(self, url: str) -> Optional[Dict]:
        """Extract text from HTML page"""
        try:
            logger.info(f"Fetching HTML: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Get title
            title = soup.title.string if soup.title else self._extract_title_from_url(url)
            
            # Extract text from main content areas
            main_content = soup.find('main') or soup.find('article') or soup.find('body')
            
            if main_content:
                text = main_content.get_text(separator='\n', strip=True)
            else:
                text = soup.get_text(separator='\n', strip=True)
            
            # Clean text
            text = self._clean_text(text)
            
            if len(text) < 100:
                logger.warning(f"Extracted text too short: {len(text)} chars")
                return None
            
            logger.info(f"Extracted {len(text)} chars from HTML")
            
            return {
                'text': text[:50000],  # Limit to 50k chars
                'url': url,
                'type': 'html',
                'title': title
            }
            
        except Exception as e:
            logger.error(f"HTML extraction failed for {url}: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        # Remove very short lines (likely navigation/UI elements)
        lines = text.split('\n')
        cleaned_lines = [line for line in lines if len(line.strip()) > 10]
        
        return '\n'.join(cleaned_lines)
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract a reasonable title from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        if path:
            # Get last part of path
            title = path.split('/')[-1]
            # Remove file extension
            title = re.sub(r'\.(pdf|html|htm)$', '', title, flags=re.IGNORECASE)
            # Replace separators with spaces
            title = re.sub(r'[-_]', ' ', title)
            return title.title()
        
        return parsed.netloc


def search_sustainability_reports(company_name: str) -> List[Dict]:
    """
    Search for sustainability reports using multiple strategies
    
    Args:
        company_name: Name of the company
        
    Returns:
        List of report URLs with metadata
    """
    reports = []
    
    # Strategy 1: Direct company website patterns
    common_patterns = [
        f'site:{company_name.lower().replace(" ", "")}.com sustainability report filetype:pdf',
        f'site:{company_name.lower().replace(" ", "")}.com ESG report filetype:pdf',
        f'site:{company_name.lower().replace(" ", "")}.com climate report filetype:pdf',
        f'site:{company_name.lower().replace(" ", "")}.com TCFD report filetype:pdf',
    ]
    
    # Strategy 2: CDP and known sustainability platforms
    cdp_patterns = [
        f'site:cdp.net "{company_name}" climate change',
        f'site:sustainability.com "{company_name}" report',
    ]
    
    # Strategy 3: SEC EDGAR for US companies (10-K filings)
    sec_patterns = [
        f'site:sec.gov "{company_name}" 10-K climate',
    ]
    
    # Note: Actual implementation would use SerpAPI to execute these searches
    # This is a placeholder for the search strategy
    
    return reports


def extract_documents_for_company(company_name: str, search_results: List[Dict]) -> List[Dict]:
    """
    Extract full documents from search results
    
    Args:
        company_name: Name of the company
        search_results: List of search results from web_search module
        
    Returns:
        List of extracted documents with full text
    """
    extractor = DocumentExtractor()
    documents = []
    
    # Prioritize PDF links
    pdf_urls = [r['url'] for r in search_results if r['url'].lower().endswith('.pdf')]
    other_urls = [r['url'] for r in search_results if not r['url'].lower().endswith('.pdf')]
    
    # Extract PDFs first (max 5)
    for url in pdf_urls[:5]:
        doc = extractor.extract_from_url(url)
        if doc:
            documents.append(doc)
            logger.info(f"Extracted PDF: {doc['title']}")
    
    # Extract HTML pages (max 10)
    for url in other_urls[:10]:
        # Skip generic domains
        if any(domain in url.lower() for domain in ['wikipedia.org', 'investopedia.com']):
            continue
        
        doc = extractor.extract_from_url(url)
        if doc:
            documents.append(doc)
            logger.info(f"Extracted HTML: {doc['title']}")
    
    logger.info(f"Extracted {len(documents)} documents for {company_name}")
    return documents


def format_documents_for_assessment(documents: List[Dict]) -> str:
    """
    Format extracted documents for AI assessment
    
    Args:
        documents: List of extracted document dicts
        
    Returns:
        Formatted string with document contents
    """
    if not documents:
        return "No documents extracted."
    
    formatted = "EXTRACTED DOCUMENTS:\n\n"
    
    for i, doc in enumerate(documents, 1):
        formatted += f"{'=' * 80}\n"
        formatted += f"DOCUMENT {i}: {doc['title']}\n"
        formatted += f"Source: {doc['url']}\n"
        formatted += f"Type: {doc['type'].upper()}\n"
        formatted += f"{'=' * 80}\n\n"
        formatted += doc['text'][:10000]  # First 10k chars per document
        formatted += "\n\n"
        
        if i >= 5:  # Limit to 5 documents to avoid token limits
            break
    
    return formatted
