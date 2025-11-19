"""
CSV Formatter - Convert assessments to detailed 271-column format
"""
import json
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

def format_detailed_csv(assessments: List[Dict]) -> List[Dict]:
    """
    Convert assessments with measures_detail JSONB to 271-column format
    
    Args:
        assessments: List of assessment records from database
        
    Returns:
        List of dicts with 271 columns (7 basic + 44 measures × 6 fields)
    """
    formatted_rows = []
    
    for assessment in assessments:
        try:
            # Basic information (7 columns)
            row = {
                'Company': assessment.get('company_name', ''),
                'ISIN': assessment.get('isin', ''),
                'Sector': assessment.get('sector', ''),
                'Industry': assessment.get('industry', ''),
                'Country': assessment.get('country', ''),
                'Assessment_Date': assessment.get('created_at', ''),
                'Overall_Risk_Rating': assessment.get('overall_risk_rating', '')
            }
            
            # Parse measures_detail JSONB
            measures_detail = assessment.get('measures_detail', {})
            if isinstance(measures_detail, str):
                try:
                    measures_detail = json.loads(measures_detail)
                except:
                    measures_detail = {}
            
            # Add all 44 measures (M01-M44) with 6 fields each
            for i in range(1, 45):
                measure_id = f"M{i:02d}"
                measure_data = measures_detail.get(measure_id, {})
                
                # Extract 6 fields for this measure
                row[f"{measure_id}_Score"] = measure_data.get('score', 0)
                row[f"{measure_id}_Confidence"] = measure_data.get('confidence', 'Unknown')
                row[f"{measure_id}_Rationale"] = measure_data.get('rationale', 'No assessment provided')
                
                # Combine evidence and source for better visibility
                evidence = measure_data.get('evidence', 'No evidence found')
                source = measure_data.get('source', '')
                
                # If source exists and is a URL, append it to evidence
                if source and source.startswith('http') and 'No evidence found' not in evidence:
                    row[f"{measure_id}_Evidence"] = f"{evidence}\n\nSource: {source}"
                else:
                    row[f"{measure_id}_Evidence"] = evidence
                
                row[f"{measure_id}_Source"] = source
                row[f"{measure_id}_AI_Model"] = measure_data.get('ai_model', 'Unknown')
            
            formatted_rows.append(row)
            
        except Exception as e:
            logger.error(f"Error formatting assessment {assessment.get('id')}: {e}")
            # Skip this row or add with defaults
            continue
    
    return formatted_rows


def get_detailed_column_order() -> List[str]:
    """
    Get the correct column order for detailed CSV (271 columns)
    
    Returns:
        List of column names in correct order
    """
    columns = [
        'Company',
        'ISIN',
        'Sector',
        'Industry',
        'Country',
        'Assessment_Date',
        'Overall_Risk_Rating'
    ]
    
    # Add all 44 measures with 6 fields each
    for i in range(1, 45):
        measure_id = f"M{i:02d}"
        columns.extend([
            f"{measure_id}_Score",
            f"{measure_id}_Confidence",
            f"{measure_id}_Rationale",
            f"{measure_id}_Evidence",
            f"{measure_id}_Source",
            f"{measure_id}_AI_Model"
        ])
    
    return columns
