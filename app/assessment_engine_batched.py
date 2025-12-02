"""
Batched Assessment Engine - DeepSeek V3 with 5-batch processing
Processes 44 measures in 5 batches (8-10 measures per batch) for comprehensive detail
"""
import os
import json
import re
import logging
from typing import Dict, List, Tuple
from openai import OpenAI
from app.brave_search import search_company_climate_info
from app.database import Database
from app.document_extraction_v3 import extract_documents_for_company
from app.document_extraction_simple import format_documents_for_assessment
from app.sustainability_portal import get_priority_documents
# from app.document_ranker import DocumentRanker  # No longer needed - using all docs without ranking

logger = logging.getLogger(__name__)

# Define measure batches (44 measures split into 5 batches)
MEASURE_BATCHES = [
    # Batch 1: Governance & Strategic Oversight (M01-M09) - 9 measures
    ["M01", "M02", "M03", "M04", "M05", "M06", "M07", "M08", "M09"],
    
    # Batch 2: Risk Identification & Assessment (M10-M18) - 9 measures  
    ["M10", "M11", "M12", "M13", "M14", "M15", "M16", "M17", "M18"],
    
    # Batch 3: Asset Design, Crisis Mgmt, Supply Chain (M19-M28) - 10 measures
    ["M19", "M20", "M21", "M22", "M23", "M24", "M25", "M26", "M27", "M28"],
    
    # Batch 4: Insurance, Data Quality, Workforce (M29-M37) - 9 measures
    ["M29", "M30", "M31", "M32", "M33", "M34", "M35", "M36", "M37"],
    
    # Batch 5: KPIs & Outcomes (M38-M44) - 7 measures
    ["M38", "M39", "M40", "M41", "M42", "M43", "M44"]
]

MEASURE_NAMES = {
    "M01": "Board Oversight of Physical Climate Risk",
    "M02": "Senior Management Responsibility for Physical Climate Risk",
    "M03": "Integration of Physical Climate Risks into Enterprise Risk Management",
    "M04": "Physical Climate Risk Strategy and Planning",
    "M05": "Physical Climate Risk Governance Structure",
    "M06": "Identification of Acute Physical Risks",
    "M07": "Identification of Chronic Physical Risks",
    "M08": "Geographic Exposure Assessment",
    "M09": "Scenario Analysis for Physical Climate Risks",
    "M10": "Quantification of Physical Climate Risk Exposure",
    "M11": "Assessment of Asset-Level Vulnerability",
    "M12": "Assessment of Supply Chain Vulnerability to Physical Risks",
    "M13": "Assessment of Operational Vulnerability",
    "M14": "Financial Impact Assessment of Physical Risks",
    "M15": "Climate-Resilient Asset Design and Construction",
    "M16": "Infrastructure Hardening and Adaptation Measures",
    "M17": "Relocation or Divestment of High-Risk Assets",
    "M18": "Nature-Based Solutions for Physical Risk Mitigation",
    "M19": "Emergency Preparedness and Response Plans",
    "M20": "Business Continuity Planning for Physical Climate Events",
    "M21": "Crisis Communication Protocols",
    "M22": "Post-Event Recovery and Restoration Capabilities",
    "M23": "Supply Chain Resilience and Diversification",
    "M24": "Supplier Climate Risk Assessment",
    "M25": "Alternative Sourcing Strategies",
    "M26": "Insurance Coverage for Physical Climate Risks",
    "M27": "Risk Transfer Mechanisms",
    "M28": "Self-Insurance and Reserves",
    "M29": "Climate Data Quality and Sources",
    "M30": "Third-Party Verification and Assurance",
    "M31": "Workforce Safety and Health Protocols",
    "M32": "Community Engagement and Support",
    "M33": "Just Transition Considerations",
    "M34": "Physical Risk KPIs and Metrics",
    "M35": "Target Setting for Physical Risk Reduction",
    "M36": "Monitoring and Reporting of Physical Risk Performance",
    "M37": "Disclosure Alignment with TCFD and Other Frameworks",
    "M38": "Demonstrated Reduction in Physical Risk Exposure",
    "M39": "Avoided Losses from Physical Climate Events",
    "M40": "Improved Asset Resilience Metrics",
    "M41": "Enhanced Operational Continuity",
    "M42": "Stakeholder Confidence and Reputation",
    "M43": "Regulatory Compliance and Preparedness",
    "M44": "Long-term Value Creation and Sustainability"
}

class BatchedAssessmentEngine:
    """Batched assessment engine using DeepSeek V3"""
    
    def __init__(self):
        self.db = Database()
        self.deepseek_client = None
        self.ai_model_name = "DeepSeek V3"
        
        # Initialize DeepSeek V3 client
        deepseek_key = os.getenv('DEEPSEEK_API_KEY')
        if not deepseek_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable not set")
        
        self.deepseek_client = OpenAI(
            api_key=deepseek_key,
            base_url="https://api.deepseek.com"
        )
        logger.info("Batched Assessment Engine initialized with DeepSeek V3")
    
    def call_deepseek(self, prompt: str, max_tokens: int = 8000) -> str:
        """Call DeepSeek V3 API"""
        try:
            response = self.deepseek_client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are an expert climate risk analyst. Provide comprehensive, evidence-based assessments with detailed multi-paragraph rationale, specific verbatim evidence quotes, and source URLs for each measure."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0  # Maximum determinism for repeatability
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"DeepSeek API call failed: {e}")
            raise
    
    def process_company(self, job_id: int, company_data: Dict):
        """Process company with batched assessment (5 API calls)"""
        company_name = company_data['name']
        isin = company_data['isin']
        
        logger.info(f"Starting batched assessment for {company_name} ({isin})")
        
        try:
            # Step 1: Get active ProcessPrompt
            processprompt = self.db.get_active_processprompt()
            if not processprompt:
                raise ValueError("No active ProcessPrompt found")
            
            processprompt_content = processprompt['content']
            logger.info(f"Using ProcessPrompt: {processprompt['version_name']}")
            
            # Step 2: Document retrieval with relevance ranking
            logger.info(f"Retrieving documents for {company_name}...")
            
            # 2a: Brave adaptive search (exhaustive)
            logger.info("[CHECKPOINT 1] Starting Brave adaptive search...")
            all_search_results = search_company_climate_info(company_name, isin=isin, max_results=150)
            logger.info(f"[CHECKPOINT 2] Brave adaptive search found {len(all_search_results)} unique documents")
            
            # 2b: Sort documents by URL for deterministic ordering
            logger.info("[CHECKPOINT 3] Sorting documents by URL for deterministic ordering...")
            # Sort by URL to ensure consistent ordering across runs
            sorted_documents = sorted(all_search_results, key=lambda x: x.get('url', ''))
            logger.info(f"[CHECKPOINT 4] Using all {len(sorted_documents)} documents (no ranking/filtering)")
            
            # 2c: Format all documents for assessment
            logger.info("[CHECKPOINT 5] Formatting all documents for LLM...")
            search_context = self._format_search_with_urls(sorted_documents)
            logger.info(f"[CHECKPOINT 6] Using {len(sorted_documents)} documents for assessment (deterministic)")
            
            # No two-pass retry - use all documents in single pass
            self._all_documents = sorted_documents
            
            # Step 3: Process each batch
            all_measures = {}
            for batch_num, measure_ids in enumerate(MEASURE_BATCHES, 1):
                logger.info(f"Processing batch {batch_num}/5 ({len(measure_ids)} measures)...")
                
                batch_prompt = self._build_batch_prompt(
                    company_data=company_data,
                    processprompt=processprompt_content,
                    web_search_results=search_context,
                    measure_ids=measure_ids,
                    batch_num=batch_num
                )
                
                # Call DeepSeek V3 for this batch
                batch_response = self.call_deepseek(batch_prompt, max_tokens=8000)
                
                # Parse batch results
                batch_measures = self._parse_batch_response(batch_response, measure_ids)
                all_measures.update(batch_measures)
                
                logger.info(f"✓ Batch {batch_num}/5 completed ({len(batch_measures)} measures)")
            
            # Step 4: Calculate overall scores (no Pass 2 retry - using all docs in single pass)
            assessment_data = self._build_assessment_data(
                all_measures=all_measures,
                company_data=company_data
            )
            
            # Step 5: Save to database
            logger.info(f"Saving detailed assessment...")
            self.db.save_assessment(job_id, company_data.get('company_id', 0), assessment_data)
            
            # Step 6: Update job status
            self.db.update_job_status(job_id, 'completed')
            
            logger.info(f"✓ Batched assessment completed for {company_name} (44 measures)")
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Assessment failed for {company_name}: {e}")
            logger.error(f"Full traceback:\n{error_trace}")
            # Store both error message and traceback
            error_msg = f"{type(e).__name__}: {str(e)}\n\nTraceback:\n{error_trace[:500]}"
            self.db.update_job_status(job_id, 'failed', error_message=error_msg)
            raise
    
    def _format_search_with_urls(self, search_results: List[Dict]) -> str:
        """Format search results with URLs preserved"""
        formatted = []
        for i, result in enumerate(search_results, 1):
            formatted.append(f"""[Source {i}]
Title: {result.get('title', 'N/A')}
URL: {result.get('url', 'N/A')}
Snippet: {result.get('snippet', 'N/A')}
""")
        return "\n".join(formatted)
    
    def _build_batch_prompt(self, company_data: Dict, processprompt: str,
                           web_search_results: str, measure_ids: List[str],
                           batch_num: int) -> str:
        """Build prompt for a specific batch of measures"""
        
        company_name = company_data['name']
        isin = company_data['isin']
        sector = company_data.get('sector', 'Unknown')
        industry = company_data.get('industry', 'Unknown')
        country = company_data.get('country', 'Unknown')
        
        # Build measure list for this batch
        measures_list = "\n".join([
            f"- **{mid}**: {MEASURE_NAMES[mid]}" 
            for mid in measure_ids
        ])
        
        # Truncate ProcessPrompt if needed
        if len(processprompt) > 40000:
            processprompt_summary = processprompt[:40000] + "\n\n[ProcessPrompt truncated]"
        else:
            processprompt_summary = processprompt
        
        prompt = f"""# Physical Climate Risk Assessment - Batch {batch_num}/5

**CRITICAL: PHYSICAL CLIMATE RISK EXCLUSIVE FOCUS**

This assessment focuses EXCLUSIVELY on **PHYSICAL CLIMATE RISKS** - the direct impacts of climate change (extreme weather, sea level rise, temperature changes, water stress, etc.).

**DO NOT accept evidence about:**
- Transition risks (policy, carbon pricing, technology, market changes)
- Generic "climate risks" or "climate-related risks" without PHYSICAL specification
- Regulatory or reputational climate risks

**ONLY accept evidence that explicitly mentions:**
- Physical climate risks, physical hazards, extreme weather events
- Climate adaptation, resilience, vulnerability to physical impacts
- Specific physical hazards (floods, hurricanes, droughts, heat, sea level rise, etc.)

**Evidence Validation Rule:**
If evidence only mentions "climate risks" or "climate-related risks" WITHOUT specifying PHYSICAL impacts, assign score 0 or "Unknown" with rationale explaining the lack of physical-risk-specific evidence.

---

## COMPANY INFORMATION
- **Company Name:** {company_name}
- **ISIN:** {isin}
- **Sector:** {sector}
- **Industry:** {industry}
- **Country:** {country}

## WEB SEARCH RESULTS (Company-Specific Climate Information)
{web_search_results}

## ASSESSMENT METHODOLOGY (ProcessPrompt v2.2)
{processprompt_summary}

## YOUR TASK

Assess the following {len(measure_ids)} measures for **{company_name}** using the ProcessPrompt v2.2 methodology:

{measures_list}

**For EACH measure, provide:**

1. **Score** (0-4):
   - 0 = No evidence found
   - 1 = Basic implementation (minimal or ad-hoc practices)
   - 2 = Developing implementation (some structured processes)
   - 3 = Structured implementation (clear processes and accountability)
   - 4 = Advanced implementation (demonstrated effectiveness and best practices)

2. **Confidence** (Low/Medium/High/Unknown): Your confidence in the score based on evidence quality

3. **Rationale** (2-4 paragraphs): Comprehensive explanation including:
   - Detailed justification for the score assigned
   - Specific references to evidence found in web results
   - Analysis of what the company is doing (or not doing)
   - What would be needed to achieve higher scores
   - Comparison to best practices where relevant

4. **Evidence** (verbatim quotes): Direct quotes from web search results
   - Use "|" to separate multiple quotes
   - Include exact text from company reports, disclosures, websites
   - Quote specific statements that support your score
   - If no evidence found, state "No evidence found"

5. **Source** (URLs): Where evidence was found
   - Use "|" to separate multiple URLs
   - Provide direct links to specific pages/reports
   - URLs should correspond to evidence quotes
   - If no evidence, leave empty

**OUTPUT FORMAT (JSON):**

```json
{{
  "measures": {{
    "{measure_ids[0]}": {{
      "score": 0-4,
      "confidence": "Low|Medium|High|Unknown",
      "rationale": "Detailed multi-paragraph explanation...",
      "evidence": "Quote 1|Quote 2|Quote 3",
      "source": "URL1|URL2|URL3"
    }},
    ... (all {len(measure_ids)} measures in this batch)
  }}
}}
```

**CRITICAL REQUIREMENTS:**
- Provide ALL {len(measure_ids)} measures in this batch
- Be thorough and evidence-based
- Use verbatim quotes for evidence
- Provide detailed rationale (2-4 paragraphs minimum per measure)
- Be realistic - score 0 if no evidence found
"""
        
        return prompt
    
    def _parse_batch_response(self, response_text: str, measure_ids: List[str]) -> Dict:
        """Parse batch response and extract measure details"""
        
        try:
            # Extract JSON from response
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
            if json_match:
                response_json = json.loads(json_match.group(1))
            else:
                # Try without code blocks
                json_match = re.search(r'\{.*"measures".*\}', response_text, re.DOTALL)
                if json_match:
                    response_json = json.loads(json_match.group(0))
                else:
                    raise ValueError("Could not find JSON in response")
            
            measures_data = response_json.get('measures', {})
            batch_measures = {}
            
            for measure_id in measure_ids:
                if measure_id in measures_data:
                    measure = measures_data[measure_id]
                    batch_measures[measure_id] = {
                        'score': int(measure.get('score', 0)),
                        'confidence': measure.get('confidence', 'Unknown'),
                        'rationale': measure.get('rationale', 'No rationale provided'),
                        'evidence': measure.get('evidence', 'No evidence found'),
                        'source': measure.get('source', ''),
                        'ai_model': self.ai_model_name
                    }
                else:
                    # Default for missing measure
                    batch_measures[measure_id] = {
                        'score': 0,
                        'confidence': 'Unknown',
                        'rationale': 'No assessment provided in batch response',
                        'evidence': 'No evidence found',
                        'source': '',
                        'ai_model': self.ai_model_name
                    }
            
            return batch_measures
            
        except Exception as e:
            logger.error(f"Failed to parse batch response: {e}")
            # Return defaults for all measures in batch
            batch_measures = {}
            for measure_id in measure_ids:
                batch_measures[measure_id] = {
                    'score': 0,
                    'confidence': 'Unknown',
                    'rationale': f'Parsing error: {str(e)}',
                    'evidence': 'No evidence found',
                    'source': '',
                    'ai_model': self.ai_model_name
                }
            return batch_measures
    
    def _build_assessment_data(self, all_measures: Dict, company_data: Dict) -> Dict:
        """Build final assessment data with all measures"""
        
        # Calculate total score
        total_score = sum(m['score'] for m in all_measures.values())
        avg_score = total_score / 44.0
        
        # Determine overall risk rating
        if avg_score >= 3.0:
            overall_risk_rating = "Low"
        elif avg_score >= 1.5:
            overall_risk_rating = "Medium"
        else:
            overall_risk_rating = "High"
        
        # Physical risk score (0-10 scale)
        physical_risk_score = round(avg_score * 2.5, 1)  # Convert 0-4 avg to 0-10
        
        return {
            'overall_risk_rating': overall_risk_rating,
            'physical_risk_score': physical_risk_score,
            'transition_risk_score': 0.0,
            'measures': all_measures,
            'total_measures_assessed': len(all_measures),
            'assessment_method': 'Batched DeepSeek V3 (5 batches)'
        }

    def _identify_retry_measures(self, all_measures: Dict) -> List[str]:
        """
        Identify measures that need retry in Pass 2
        
        Criteria for retry:
        - Score = 0 (no evidence found)
        - Evidence length < 50 chars (weak evidence)
        - Score = "Unknown" or "N/A"
        
        Returns:
            List of measure IDs to retry
        """
        retry_measures = []
        
        for measure_id, data in all_measures.items():
            score = data.get('score', 0)
            evidence = data.get('evidence', '')
            
            # Check if retry needed
            should_retry = False
            
            # Criterion 1: Score is 0 or Unknown
            if score == 0 or score in ['Unknown', 'N/A', 'unknown', 'n/a']:
                should_retry = True
            
            # Criterion 2: Weak evidence (< 50 chars)
            elif isinstance(evidence, str) and len(evidence.strip()) < 50:
                should_retry = True
            
            if should_retry:
                retry_measures.append(measure_id)
                logger.debug(f"Retry candidate: {measure_id} (score={score}, evidence_len={len(str(evidence))})")
        
        return retry_measures
    
    def _is_better_result(self, new_data: Dict, old_data: Dict) -> bool:
        """
        Determine if new result is better than old result
        
        Better means:
        - Higher score (0 < 1 < 2 < 3)
        - Longer/more detailed evidence
        - More specific rationale
        
        Returns:
            True if new result should replace old result
        """
        if not old_data:
            return True
        
        old_score = old_data.get('score', 0)
        new_score = new_data.get('score', 0)
        
        # Convert Unknown/N/A to 0 for comparison
        if old_score in ['Unknown', 'N/A', 'unknown', 'n/a']:
            old_score = 0
        if new_score in ['Unknown', 'N/A', 'unknown', 'n/a']:
            new_score = 0
        
        # Higher score is always better
        if new_score > old_score:
            return True
        
        # If same score, longer evidence is better
        if new_score == old_score:
            old_evidence_len = len(str(old_data.get('evidence', '')))
            new_evidence_len = len(str(new_data.get('evidence', '')))
            
            if new_evidence_len > old_evidence_len * 1.5:  # At least 50% more evidence
                return True
        
        return False
