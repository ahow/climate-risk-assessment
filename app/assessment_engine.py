"""
Assessment Engine - DeepSeek V3 with ProcessPrompt v2.2
Performs comprehensive physical climate risk assessments using 44-measure framework
"""
import os
import json
import re
import logging
from typing import Dict, List
from openai import OpenAI
from app.web_search import search_company_climate_info, format_search_results
from app.database import Database

logger = logging.getLogger(__name__)

class AssessmentEngine:
    """Climate risk assessment engine using DeepSeek V3 and ProcessPrompt v2.2"""
    
    def __init__(self):
        self.db = Database()
        self.deepseek_client = None
        
        # Initialize DeepSeek V3 client
        deepseek_key = os.getenv('DEEPSEEK_API_KEY')
        if not deepseek_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable not set")
        
        self.deepseek_client = OpenAI(
            api_key=deepseek_key,
            base_url="https://api.deepseek.com"
        )
        logger.info("DeepSeek V3 client initialized")
    
    def call_deepseek(self, prompt: str, max_tokens: int = 8000) -> str:
        """
        Call DeepSeek V3 API
        
        Args:
            prompt: The prompt to send
            max_tokens: Maximum tokens in response
            
        Returns:
            Model response text
        """
        try:
            response = self.deepseek_client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are an expert climate risk analyst specializing in physical climate risk assessment using the ProcessPrompt v2.2 methodology. You provide evidence-based, structured assessments with specific scores for all 44 measures."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.3  # Lower temperature for more consistent scoring
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"DeepSeek API call failed: {e}")
            raise
    
    def process_company(self, job_id: int, company_data: Dict):
        """
        Process a single company assessment using ProcessPrompt v2.2
        
        Args:
            job_id: Assessment job ID
            company_data: Dict with company info (name, isin, sector, industry, country)
        """
        company_name = company_data['name']
        isin = company_data['isin']
        
        logger.info(f"Starting ProcessPrompt v2.2 assessment for {company_name} ({isin})")
        
        try:
            # Step 1: Get active ProcessPrompt
            processprompt = self.db.get_active_processprompt()
            if not processprompt:
                # Load ProcessPrompt v2.2 from file
                self._load_processprompt_v22()
                processprompt = self.db.get_active_processprompt()
            
            processprompt_content = processprompt['content']
            logger.info(f"Using ProcessPrompt: {processprompt['version_name']} ({len(processprompt_content)} chars)")
            
            # Step 2: Perform company-specific web search
            logger.info(f"Searching web for {company_name}...")
            search_results = search_company_climate_info(company_name, max_results=15)
            search_context = format_search_results(search_results)
            logger.info(f"Found {len(search_results)} relevant web results")
            
            # Step 3: Build comprehensive prompt with ProcessPrompt methodology
            assessment_prompt = self._build_processprompt_assessment(
                company_data=company_data,
                processprompt=processprompt_content,
                web_search_results=search_context
            )
            
            # Step 4: Call DeepSeek V3 for assessment
            logger.info(f"Calling DeepSeek V3 for ProcessPrompt v2.2 assessment...")
            assessment_text = self.call_deepseek(assessment_prompt, max_tokens=8000)
            
            # Step 5: Parse and structure the assessment
            assessment_data = self._parse_processprompt_assessment(assessment_text, company_data)
            
            # Step 6: Save to database
            logger.info(f"Saving assessment to database...")
            self.db.save_assessment(job_id, company_data.get('company_id', 0), assessment_data)
            
            # Step 7: Update job status
            self.db.update_job_status(job_id, 'completed')
            
            logger.info(f"✓ ProcessPrompt v2.2 assessment completed for {company_name}")
            
        except Exception as e:
            logger.error(f"Assessment failed for {company_name}: {e}", exc_info=True)
            self.db.update_job_status(job_id, 'failed', error_message=str(e))
            raise
    
    def _build_processprompt_assessment(self, company_data: Dict, processprompt: str, 
                                       web_search_results: str) -> str:
        """Build the complete ProcessPrompt v2.2 assessment prompt"""
        
        company_name = company_data['name']
        isin = company_data['isin']
        sector = company_data.get('sector', 'Unknown')
        industry = company_data.get('industry', 'Unknown')
        country = company_data.get('country', 'Unknown')
        
        # Truncate ProcessPrompt if too long (keep key sections)
        if len(processprompt) > 50000:
            # Extract key sections: measure definitions, scoring criteria
            processprompt_summary = self._extract_key_sections(processprompt)
        else:
            processprompt_summary = processprompt
        
        prompt = f"""# Physical Climate Risk Assessment - ProcessPrompt v2.2

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

Conduct a comprehensive physical climate risk assessment for **{company_name}** using the ProcessPrompt v2.2 methodology above.

**CRITICAL REQUIREMENTS:**

1. **Assess ALL 44 measures** across the 9 categories
2. **Score each measure 0-5** based on available evidence
3. **Use EXACT measure names** from ProcessPrompt (e.g., "M01: Board Oversight of Physical Climate Risk")
4. **Provide evidence** for each score (verbatim quotes from web results where available)
5. **Be realistic** - score 0 if no evidence found, don't assume practices exist

**OUTPUT FORMAT (REQUIRED):**

```json
{{
  "overall_risk_rating": "Low|Medium|High",
  "total_score": 0-220,
  "physical_risk_score": 0-10,
  "category_scores": {{
    "Governance & Strategic Oversight": 0-35,
    "Risk Identification & Assessment": 0-45,
    "Asset Design & Resilience": 0-25,
    "Crisis Management": 0-25,
    "Supply Chain Management": 0-25,
    "Insurance & Risk Transfer": 0-20,
    "Data Quality & Assurance": 0-10,
    "Workforce & Community": 0-15,
    "KPIs & Outcomes": 0-20
  }},
  "measures": [
    {{
      "measure_id": "M01",
      "measure_name": "Board Oversight of Physical Climate Risk",
      "category": "Governance & Strategic Oversight",
      "score": 0-5,
      "evidence": "Verbatim quote or 'No evidence found'",
      "rationale": "Brief explanation of score"
    }},
    ... (all 44 measures)
  ],
  "executive_summary": "Brief 2-3 paragraph summary of key findings",
  "key_risks": ["Risk 1", "Risk 2", "Risk 3"],
  "adaptation_strengths": ["Strength 1", "Strength 2"],
  "recommendations": ["Rec 1", "Rec 2", "Rec 3"]
}}
```

**IMPORTANT:** 
- Return ONLY the JSON object, no additional text
- Include all 44 measures (M01-M44)
- Base scores on actual evidence from web search results
- Be conservative: no evidence = score 0
- Use exact measure names from ProcessPrompt

Begin assessment now.
"""
        
        return prompt
    
    def _extract_key_sections(self, processprompt: str) -> str:
        """Extract key sections from ProcessPrompt to fit token limits"""
        
        # Extract measure definitions section (most important)
        sections_to_keep = []
        
        # Keep objective and scope
        if "## OBJECTIVE" in processprompt:
            obj_start = processprompt.find("## OBJECTIVE")
            obj_end = processprompt.find("## SCOPE", obj_start)
            if obj_end > obj_start:
                sections_to_keep.append(processprompt[obj_start:obj_end])
        
        # Keep scoring framework
        if "## SCORING FRAMEWORK" in processprompt or "### 3. Risk Rating Scale" in processprompt:
            score_start = max(
                processprompt.find("## SCORING FRAMEWORK"),
                processprompt.find("### 3. Risk Rating Scale")
            )
            if score_start > 0:
                score_section = processprompt[score_start:score_start+2000]
                sections_to_keep.append(score_section)
        
        # Keep measure names (critical)
        if "## EXPANDED MEASURE DEFINITIONS" in processprompt or "### The 9 Categories" in processprompt:
            measures_start = max(
                processprompt.find("## EXPANDED MEASURE DEFINITIONS"),
                processprompt.find("### The 9 Categories")
            )
            if measures_start > 0:
                measures_section = processprompt[measures_start:measures_start+10000]
                sections_to_keep.append(measures_section)
        
        return "\n\n".join(sections_to_keep) if sections_to_keep else processprompt[:20000]
    
    def _parse_processprompt_assessment(self, assessment_text: str, company_data: Dict) -> Dict:
        """Parse ProcessPrompt v2.2 assessment JSON response"""
        
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', assessment_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find raw JSON
                json_match = re.search(r'\{.*"measures".*\}', assessment_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    raise ValueError("No valid JSON found in assessment response")
            
            # Parse JSON
            assessment_json = json.loads(json_str)
            
            # Extract key fields
            overall_rating = assessment_json.get('overall_risk_rating', 'Medium')
            total_score = assessment_json.get('total_score', 0)
            physical_risk_score = assessment_json.get('physical_risk_score', 5.0)
            
            # Calculate average score from measures if available
            measures = assessment_json.get('measures', [])
            if measures:
                avg_score = sum(m.get('score', 0) for m in measures) / len(measures)
                physical_risk_score = round(avg_score, 1)
            
            # Convert measures list to dict format for CSV formatter
            measures_detail = {}
            for measure in measures:
                measure_id = measure.get('measure_id', '')
                if measure_id:
                    measures_detail[measure_id] = {
                        'score': measure.get('score', 0),
                        'confidence': measure.get('confidence', 'Unknown'),
                        'rationale': measure.get('rationale', 'No assessment provided'),
                        'evidence': measure.get('evidence', 'No evidence found'),
                        'source': measure.get('source', ''),
                        'ai_model': 'DeepSeek V3'
                    }
            
            # Store full assessment
            full_assessment = {
                'json_assessment': assessment_json,
                'raw_text': assessment_text,
                'total_measures': len(measures),
                'measures_assessed': len([m for m in measures if m.get('score', 0) > 0])
            }
            
            return {
                'overall_risk_rating': overall_rating,
                'physical_risk_score': physical_risk_score,
                'transition_risk_score': 0.0,  # Not assessed (physical risk only)
                'full_assessment': json.dumps(full_assessment, indent=2),
                'measures': measures_detail,  # Add measures_detail for CSV export
                'company_name': company_data['name'],
                'isin': company_data['isin'],
                'total_score': total_score,
                'measures_count': len(measures)
            }
            
        except Exception as e:
            logger.error(f"Failed to parse ProcessPrompt assessment: {e}")
            logger.error(f"Assessment text: {assessment_text[:500]}...")
            
            # Fallback: return basic structure with raw text
            return {
                'overall_risk_rating': 'Medium',
                'physical_risk_score': 5.0,
                'transition_risk_score': 0.0,
                'full_assessment': assessment_text,
                'company_name': company_data['name'],
                'isin': company_data['isin'],
                'total_score': 0,
                'measures_count': 0,
                'parse_error': str(e)
            }
    
    def _load_processprompt_v22(self):
        """Load ProcessPrompt v2.2 from file into database"""
        
        processprompt_path = '/home/ubuntu/climate-improved/ProcessPrompt_v2.2.md'
        
        if not os.path.exists(processprompt_path):
            logger.warning(f"ProcessPrompt v2.2 file not found at {processprompt_path}, using default")
            self._create_default_processprompt()
            return
        
        try:
            with open(processprompt_path, 'r', encoding='utf-8') as f:
                processprompt_content = f.read()
            
            file_size = len(processprompt_content)
            logger.info(f"Loaded ProcessPrompt v2.2: {file_size} bytes")
            
            conn = self.db.get_connection()
            try:
                with conn.cursor() as cursor:
                    # Deactivate existing ProcessPrompts
                    cursor.execute("UPDATE processprompt_versions SET is_active = FALSE")
                    
                    # Insert new ProcessPrompt
                    cursor.execute("""
                        INSERT INTO processprompt_versions (version_name, content, is_active, notes)
                        VALUES (%s, %s, TRUE, %s)
                    """, (
                        'ProcessPrompt_20251105_225650',
                        processprompt_content,
                        f'ProcessPrompt v2.2 loaded from file ({file_size} bytes)'
                    ))
                    
                    conn.commit()
                    logger.info("ProcessPrompt v2.2 loaded into database")
                    
            finally:
                self.db.release_connection(conn)
                
        except Exception as e:
            logger.error(f"Failed to load ProcessPrompt v2.2: {e}")
            self._create_default_processprompt()
    
    def _create_default_processprompt(self):
        """Create simplified default ProcessPrompt if v2.2 not available"""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cursor:
                default_content = """# ProcessPrompt v2.2 - Physical Climate Risk Assessment

## Assessment Framework

### The 9 Categories and 44 Measures

1. **Governance & Strategic Oversight** (M01-M07)
   - M01: Board Oversight of Physical Climate Risk
   - M02: Management Responsibility for Physical Climate Risk
   - M03: Integration into Enterprise Risk Management
   - M04: Formal Commitments and Policies
   - M05: Scenario Analysis and Forward-Looking Assessment
   - M06: Stakeholder Engagement on Physical Risk
   - M07: Policy Advocacy and Industry Collaboration

2. **Risk Identification & Assessment** (M08-M16)
   - M08: Hazard Identification
   - M09: Asset-Level Exposure Assessment
   - M10: Vulnerability Assessment
   - M11: Scenario-Based Modeling
   - M12: Financial Quantification of Physical Risks
   - M13: Supply Chain Climate Risk Assessment
   - M14: Third-Party Validation of Risk Assessments
   - M15: Regulatory Compliance and Reporting
   - M16: Quality and Granularity of Disclosure

3. **Asset Design & Resilience** (M17-M21)
   - M17: Climate-Resilient Design Standards
   - M18: Retrofitting and Hardening Programs
   - M19: Nature-Based Solutions
   - M20: Critical Infrastructure Protection
   - M21: Strategic Relocation or Divestment

4. **Crisis Management** (M22-M26)
   - M22: Business Continuity Plans
   - M23: Emergency Response Protocols
   - M24: Crisis Communication Systems
   - M25: Recovery Time Objectives
   - M26: Post-Event Review and Learning

5. **Supply Chain Management** (M27-M31)
   - M27: Supplier Climate Risk Assessment
   - M28: Geographic Diversification
   - M29: Contractual Risk-Sharing Provisions
   - M30: Buffer Inventory and Safety Stock
   - M31: Logistics and Transportation Flexibility

6. **Insurance & Risk Transfer** (M32-M35)
   - M32: Insurance Coverage Adequacy
   - M33: Parametric Insurance Products
   - M34: Captive Insurance or Self-Insurance
   - M35: Claims Management and Recovery

7. **Data Quality & Assurance** (M36-M37)
   - M36: Climate Data Governance
   - M37: External Assurance of Climate Risk Data

8. **Workforce & Community** (M38-M40)
   - M38: Employee Safety and Welfare
   - M39: Community Engagement and Support
   - M40: Just Transition Considerations

9. **KPIs & Outcomes** (M41-M44)
   - M41: Operational Downtime Metrics
   - M42: Financial Impact Disclosure
   - M43: Supply Chain Disruption Metrics
   - M44: Adaptation Investment and Spend

### Scoring Scale (0-5 for each measure)
- **0**: No evidence of practice
- **1**: Minimal/ad-hoc approach
- **2**: Basic formal process
- **3**: Systematic implementation
- **4**: Advanced practices with metrics
- **5**: Leading practice with continuous improvement

### Assessment Instructions
1. Review all available company documents and web sources
2. Score each of the 44 measures based on evidence
3. Provide verbatim quotes as evidence where possible
4. Be realistic - no evidence means score 0
5. Calculate total score (max 220 points)
6. Determine overall risk rating based on total score
"""
                
                cursor.execute("""
                    INSERT INTO processprompt_versions (version_name, content, is_active, notes)
                    VALUES (%s, %s, TRUE, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    'ProcessPrompt_v2.2_Default',
                    default_content,
                    'Default ProcessPrompt v2.2 loaded at system startup'
                ))
                
                conn.commit()
                logger.info("Default ProcessPrompt v2.2 created")
                
        finally:
            self.db.release_connection(conn)
