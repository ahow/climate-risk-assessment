"""
External Spreadsheet Sync Module
Handles importing companies from external API
"""
import requests
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

EXTERNAL_API_URL = "https://climaterisk-rur7xaeu.manus.space/api/trpc/companies.list"

def fetch_external_companies() -> Optional[List[Dict]]:
    """
    Fetch company list from external tRPC API
    
    Returns:
        List of company dictionaries, or None if fetch fails
    """
    try:
        logger.info(f"Fetching companies from {EXTERNAL_API_URL}")
        
        response = requests.get(EXTERNAL_API_URL, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract companies from tRPC response structure
        if 'result' in data and 'data' in data['result'] and 'json' in data['result']['data']:
            companies = data['result']['data']['json']
            logger.info(f"Successfully fetched {len(companies)} companies from API")
            return companies
        else:
            logger.error("Unexpected API response structure")
            return None
        
    except Exception as e:
        logger.error(f"Failed to fetch companies from API: {e}")
        return None

def parse_companies_from_api(api_companies: List[Dict]) -> List[Dict]:
    """
    Parse company data from API response
    
    Args:
        api_companies: List of company dictionaries from API
        
    Returns:
        List of company dictionaries with standardized format
    """
    companies = []
    
    for idx, company_dict in enumerate(api_companies):
        try:
            company = {
                'name': str(company_dict.get('name', '')).strip(),
                'isin': str(company_dict.get('isin', '')).strip(),
                'sector': str(company_dict.get('sector', '')).strip() or None,
                'industry': str(company_dict.get('sector', '')).strip() or None,  # Using sector as industry
                'country': str(company_dict.get('geography', '')).strip() or None,
            }
            
            # Validate required fields
            if company['name'] and company['isin']:
                companies.append(company)
            else:
                logger.warning(f"Skipping company {idx}: missing name or ISIN")
                
        except Exception as e:
            logger.error(f"Error parsing company {idx}: {e}")
            continue
    
    logger.info(f"Parsed {len(companies)} valid companies from API")
    return companies

def sync_companies_from_external(db) -> Dict:
    """
    Sync companies from external API to database
    
    Args:
        db: Database instance
        
    Returns:
        Dictionary with sync results (added, updated, skipped, errors)
    """
    results = {
        'success': False,
        'added': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0,
        'total': 0,
        'message': ''
    }
    
    try:
        # Fetch companies from API
        api_companies = fetch_external_companies()
        if api_companies is None:
            results['message'] = "Failed to fetch companies from API"
            return results
        
        # Parse companies
        companies = parse_companies_from_api(api_companies)
        results['total'] = len(companies)
        
        if not companies:
            results['message'] = "No valid companies found in API response"
            return results
        
        # Sync each company to database
        for company in companies:
            try:
                # Check if company exists
                existing = db.get_company_by_isin(company['isin'])
                
                if existing:
                    # Update existing company
                    db.update_company(
                        company_id=existing['id'],
                        name=company['name'],
                        sector=company.get('sector'),
                        industry=company.get('industry'),
                        country=company.get('country')
                    )
                    results['updated'] += 1
                    logger.info(f"Updated company: {company['name']} ({company['isin']})")
                else:
                    # Add new company
                    db.add_company(
                        name=company['name'],
                        isin=company['isin'],
                        sector=company.get('sector'),
                        industry=company.get('industry'),
                        country=company.get('country')
                    )
                    results['added'] += 1
                    logger.info(f"Added company: {company['name']} ({company['isin']})")
                    
            except Exception as e:
                logger.error(f"Error syncing company {company['name']}: {e}")
                results['errors'] += 1
        
        results['success'] = True
        results['message'] = f"Synced {results['added']} new, {results['updated']} updated, {results['errors']} errors"
        
    except Exception as e:
        logger.error(f"External sync failed: {e}")
        results['message'] = f"Sync failed: {str(e)}"
    
    return results

def submit_assessments_for_companies(db, company_isins: List[str] = None) -> Dict:
    """
    Submit assessment jobs for companies
    
    Args:
        db: Database instance
        company_isins: Optional list of ISINs to assess. If None, assesses all companies.
        
    Returns:
        Dictionary with submission results
    """
    results = {
        'success': False,
        'submitted': 0,
        'skipped': 0,
        'errors': 0,
        'message': ''
    }
    
    try:
        # Get companies to assess
        if company_isins:
            companies = [db.get_company_by_isin(isin) for isin in company_isins]
            companies = [c for c in companies if c]  # Filter out None
        else:
            companies = db.get_all_companies()
        
        if not companies:
            results['message'] = "No companies found to assess"
            return results
        
        # Submit assessment job for each company
        for company in companies:
            try:
                # Check if there's already a pending/processing job
                existing_jobs = db.get_company_jobs(company['id'])
                has_pending = any(j['status'] in ['pending', 'processing'] for j in existing_jobs)
                
                if has_pending:
                    results['skipped'] += 1
                    logger.info(f"Skipped {company['name']}: already has pending job")
                    continue
                
                # Submit new job
                job_id = db.create_assessment_job(company['id'])
                results['submitted'] += 1
                logger.info(f"Submitted assessment job for {company['name']} (Job ID: {job_id})")
                
            except Exception as e:
                logger.error(f"Error submitting job for {company['name']}: {e}")
                results['errors'] += 1
        
        results['success'] = True
        results['message'] = f"Submitted {results['submitted']} jobs, {results['skipped']} skipped, {results['errors']} errors"
        
    except Exception as e:
        logger.error(f"Assessment submission failed: {e}")
        results['message'] = f"Submission failed: {str(e)}"
    
    return results
