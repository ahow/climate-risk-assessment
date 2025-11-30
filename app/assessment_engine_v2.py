"""
Assessment Engine v2 - Detailed Measure Extraction
Extracts all 6 fields per measure: score, confidence, rationale, evidence, source, ai_model
"""
import os
import json
import re
import logging
from typing import Dict, List
from openai import OpenAI
from app.brave_search import search_company_climate_info, format_search_results
from app.database import Database

logger = logging.getLogger(__name__)

class AssessmentEngineV2:
    """Enhanced assessment engine with detailed measure extraction"""
    
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
        logger.info("DeepSeek V3 client initialized (v2)")
    
    def call_deepseek(self, prompt: str, max_tokens: int = 16000) -> str:
        """Call DeepSeek V3 API with higher token limit for detailed output"""
        try:
            response = self.deepseek_client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are an expert climate risk analyst. Provide comprehensive, evidence-based assessments with detailed rationale, specific evidence quotes, and source URLs for each measure."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.3
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"DeepSeek API call failed: {e}")
            raise
    
    def process_company(self, job_id: int, company_data: Dict):
        """Process company with detailed measure extraction"""
        company_name = company_data['name']
        isin = company_data['isin']
        
        logger.info(f"Starting detailed assessment for {company_name} ({isin})")
        
        try:
            # Step 1: Get active ProcessPrompt
            processprompt = self.db.get_active_processprompt()
            if not processprompt:
                raise ValueError("No active ProcessPrompt found")
            
            processprompt_content = processprompt['content']
            logger.info(f"Using ProcessPrompt: {processprompt['version_name']}")
            
            # Step 2: Web search
            logger.info(f"Searching web for {company_name}...")
            search_results = search_company_climate_info(company_name, max_results=15)
            search_context = self._format_search_with_urls(search_results)
            logger.info(f"Found {len(search_results)} relevant results")
            
            # Step 3: Build detailed prompt
            assessment_prompt = self._build_detailed_prompt(
                company_data=company_data,
                processprompt=processprompt_content,
                web_search_results=search_context
            )
            
            # Step 4: Call DeepSeek V3
            logger.info(f"Calling DeepSeek V3 for detailed assessment...")
            assessment_text = self.call_deepseek(assessment_prompt, max_tokens=16000)
            
            # Step 5: Parse detailed measures
            assessment_data = self._parse_detailed_assessment(
                assessment_text, 
                company_data,
                search_results
            )
            
            # Step 6: Save to database
            logger.info(f"Saving detailed assessment...")
            self.db.save_assessment(job_id, company_data.get('company_id', 0), assessment_data)
            
            # Step 7: Update job status
            self.db.update_job_status(job_id, 'completed')
            
            logger.info(f"✓ Detailed assessment completed for {company_name}")
            
        except Exception as e:
            logger.error(f"Assessment failed for {company_name}: {e}", exc_info=True)
            self.db.update_job_status(job_id, 'failed', error_message=str(e))
            raise
    
    def _format_search_with_urls(self, search_results: List[Dict]) -> str:
        """Format search results with URLs preserved"""
        formatted = []
        for i, result in enumerate(search_results, 1):
            formatted.append(f"""
[Source {i}]
Title: {result.get('title', 'N/A')}
URL: {result.get('url', 'N/A')}
Snippet: {result.get('snippet', 'N/A')}
""")
        return "\n".join(formatted)
    
    def _build_detailed_prompt(self, company_data: Dict, processprompt: str, 
                              web_search_results: str) -> str:
        """Build prompt requesting detailed output for all measures"""
        
        company_name = company_data['name']
        isin = company_data['isin']
        sector = company_data.get('sector', 'Unknown')
        industry = company_data.get('industry', 'Unknown')
        country = company_data.get('country', 'Unknown')
        
        # Truncate ProcessPrompt if needed
        if len(processprompt) > 50000:
            processprompt_summary = processprompt[:50000] + "\n\n[ProcessPrompt truncated for length]"
        else:
            processprompt_summary = processprompt
        
        prompt = f"""# Comprehensive Physical Climate Risk Assessment

## COMPANY INFORMATION
- **Company Name:** {company_name}
- **ISIN:** {isin}
- **Sector:** {sector}
- **Industry:** {industry}
- **Country:** {country}

## WEB SEARCH RESULTS
{web_search_results}

## ASSESSMENT METHODOLOGY (ProcessPrompt v2.2)
{processprompt_summary}

## YOUR TASK

Conduct a comprehensive assessment for **{company_name}** using the ProcessPrompt v2.2 methodology.

**For EACH of the 44 measures, provide:**

1. **Score** (0-4): Based on evidence
   - 0 = No evidence
   - 1 = Basic implementation
   - 2 = Developing implementation
   - 3 = Structured implementation
   - 4 = Advanced implementation

2. **Confidence** (Low/Medium/High/Unknown): Your confidence in the score

3. **Rationale** (2-4 paragraphs): Detailed explanation including:
   - Why this score was assigned
   - What evidence supports it
   - What would be needed for higher/lower scores
   - Specific references to company disclosures

4. **Evidence** (verbatim quotes): Direct quotes from web results
   - Multiple quotes separated by "|"
   - Use exact text from sources
   - Include company statements, report excerpts, etc.

5. **Source** (URLs): Where evidence was found
   - Multiple URLs separated by "|"
   - Direct links to reports/pages
   - Correspond to evidence quotes

**OUTPUT FORMAT (JSON):**

```json
{{
  "overall_risk_rating": "Low|Medium|High",
  "measures": {{
    "M01": {{
      "score": 0-4,
      "confidence": "Low|Medium|High|Unknown",
      "rationale": "Multi-paragraph detailed explanation...",
      "evidence": "Quote 1|Quote 2|Quote 3",
      "source": "URL1|URL2|URL3"
    }},
    "M02": {{ ... }},
    ... (all 44 measures M01-M44)
  }}
}}
```

**CRITICAL:** Provide ALL 44 measures (M01-M44) with complete details. Be thorough and evidence-based.
"""
        
        return prompt
    
    def _parse_detailed_assessment(self, assessment_text: str, company_data: Dict,
                                   search_results: List[Dict]) -> Dict:
        """Parse detailed assessment with all measure fields"""
        
        try:
            # Extract JSON from response
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', assessment_text, re.DOTALL)
            if json_match:
                assessment_json = json.loads(json_match.group(1))
            else:
                # Try to find JSON without code blocks
                json_match = re.search(r'\{.*"measures".*\}', assessment_text, re.DOTALL)
                if json_match:
                    assessment_json = json.loads(json_match.group(0))
                else:
                    raise ValueError("Could not find JSON in response")
            
            # Build measures detail with all 6 fields
            measures_detail = {}
            measures_data = assessment_json.get('measures', {})
            
            for measure_id in [f"M{i:02d}" for i in range(1, 45)]:
                if measure_id in measures_data:
                    measure = measures_data[measure_id]
                    measures_detail[measure_id] = {
                        'score': measure.get('score', 0),
                        'confidence': measure.get('confidence', 'Unknown'),
                        'rationale': measure.get('rationale', 'No rationale provided'),
                        'evidence': measure.get('evidence', 'No evidence found'),
                        'source': measure.get('source', ''),
                        'ai_model': self.ai_model_name
                    }
                else:
                    # Default for missing measures
                    measures_detail[measure_id] = {
                        'score': 0,
                        'confidence': 'Unknown',
                        'rationale': 'No assessment provided',
                        'evidence': 'No evidence found',
                        'source': '',
                        'ai_model': self.ai_model_name
                    }
            
            # Calculate scores
            total_score = sum(m['score'] for m in measures_detail.values())
            physical_risk_score = round(total_score / 44.0 * 10, 1)  # Normalize to 0-10
            
            return {
                'overall_risk_rating': assessment_json.get('overall_risk_rating', 'Unknown'),
                'physical_risk_score': physical_risk_score,
                'transition_risk_score': 0.0,  # Not assessed in this version
                'measures': measures_detail,
                'full_assessment_text': assessment_text
            }
            
        except Exception as e:
            logger.error(f"Failed to parse assessment: {e}")
            # Return default structure
            measures_detail = {}
            for measure_id in [f"M{i:02d}" for i in range(1, 45)]:
                measures_detail[measure_id] = {
                    'score': 0,
                    'confidence': 'Unknown',
                    'rationale': f'Parsing error: {str(e)}',
                    'evidence': 'No evidence found',
                    'source': '',
                    'ai_model': self.ai_model_name
                }
            
            return {
                'overall_risk_rating': 'Unknown',
                'physical_risk_score': 0.0,
                'transition_risk_score': 0.0,
                'measures': measures_detail,
                'full_assessment_text': assessment_text
            }
