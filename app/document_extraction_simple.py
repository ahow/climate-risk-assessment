"""
Simple Document Extraction - Using Jina AI Reader API
No external dependencies required - works with any URL including PDFs
"""
import logging
import requests
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def extract_text_from_url(url: str) -> Optional[Dict]:
    """
    Extract full text from URL using Jina AI Reader API
    Works with PDFs, HTML, and most web content
    
    Args:
        url: URL to extract from
        
    Returns:
        Dict with 'text', 'url', 'title' or None if failed
    """
    try:
        # Jina AI Reader API - free, no API key required
        reader_url = f"https://r.jina.ai/{url}"
        
        logger.info(f"Extracting text from: {url}")
        
        response = requests.get(
            reader_url,
            headers={'Accept': 'application/json'},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            # Jina AI returns {"data": {"content": "...", "title": "..."}}
            data = result.get('data', {})
            text = data.get('content', '')
            title = data.get('title', url.split('/')[-1])
            
            if len(text) < 100:
                logger.warning(f"Extracted text too short: {len(text)} chars")
                return None
            
            logger.info(f"✓ Extracted {len(text)} characters from {url}")
            
            return {
                'text': text[:50000],  # Limit to 50k chars
                'url': url,
                'title': title,
                'type': 'pdf' if url.lower().endswith('.pdf') else 'html'
            }
        else:
            logger.warning(f"Jina Reader failed: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to extract from {url}: {e}")
        return None


def extract_documents_for_company(company_name: str, urls: List[Dict]) -> List[Dict]:
    """
    Extract full documents from URLs
    
    Args:
        company_name: Name of the company
        urls: List of URL dicts from search/portal
        
    Returns:
        List of extracted documents with full text
    """
    documents = []
    
    # Prioritize PDFs
    pdf_urls = [u.get('url') for u in urls if u.get('url', '').lower().endswith('.pdf')]
    other_urls = [u.get('url') for u in urls if not u.get('url', '').lower().endswith('.pdf')]
    
    # Extract PDFs first (max 5)
    for url in pdf_urls[:5]:
        doc = extract_text_from_url(url)
        if doc:
            documents.append(doc)
            if len(documents) >= 5:  # Limit total documents
                break
    
    # Extract HTML pages if we need more
    if len(documents) < 5:
        for url in other_urls[:10]:
            # Skip generic domains
            if any(domain in url.lower() for domain in ['wikipedia.org', 'investopedia.com']):
                continue
            
            doc = extract_text_from_url(url)
            if doc:
                documents.append(doc)
                if len(documents) >= 5:
                    break
    
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
        
        if i >= 5:  # Limit to 5 documents
            break
    
    return formatted
