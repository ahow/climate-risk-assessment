"""
Measure-Specific Extraction Prompts
IMPROVEMENT #6: Tailored prompts for commonly-missing measures

This module provides specialized extraction guidance for measures
that typically have low evidence discovery rates.
"""

# Measure-specific search guidance and acceptable evidence patterns
MEASURE_SPECIFIC_GUIDANCE = {
    # ===================================================================
    # CATEGORY 3: Asset Design & Resilience (M17-M21)
    # Currently: 4-12% evidence discovery
    # Target: 30-50% evidence discovery
    # ===================================================================
    "M17": {
        "name": "Climate-resilient design standards",
        "search_keywords": [
            "engineering standards", "design guidelines", "building codes",
            "climate-resilient design", "resilient infrastructure",
            "design criteria", "construction standards", "facility design"
        ],
        "acceptable_evidence": [
            "Design standards that incorporate climate projections",
            "Engineering guidelines for extreme weather",
            "Building codes that exceed standard requirements",
            "Facility design criteria for floods/storms/heat",
            "Infrastructure standards for climate adaptation"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "engineering standards", "design guidelines", "building codes"
- Look for "climate-resilient design", "resilient infrastructure"
- Look for specific hazard design criteria (flood-resistant, wind-resistant, heat-resistant)

ACCEPTABLE EVIDENCE:
- Design standards that mention climate/weather considerations
- Engineering guidelines that incorporate future climate projections
- Building codes that exceed standard requirements for resilience
- Facility design criteria that address extreme weather
- Infrastructure standards that mention adaptation

SCORING GUIDANCE:
- Score 1-2: Generic mention of design standards without climate specificity
- Score 3: Design standards that mention climate/weather considerations
- Score 4: Comprehensive standards with future climate projections
- Score 5: Group-wide standards with specific hazard criteria and validation
"""
    },
    
    "M18": {
        "name": "Retrofitting & adaptation programs",
        "search_keywords": [
            "retrofitting", "facility upgrades", "infrastructure adaptation",
            "building improvements", "asset hardening", "facility modernization",
            "infrastructure investment", "capital improvements"
        ],
        "acceptable_evidence": [
            "Retrofitting programs for existing facilities",
            "Facility upgrades to withstand extreme weather",
            "Infrastructure adaptation investments",
            "Building improvements for climate resilience",
            "Asset hardening programs"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "retrofitting", "facility upgrades", "infrastructure adaptation"
- Look for "building improvements", "asset hardening", "modernization"
- Look for capital expenditure on climate adaptation

ACCEPTABLE EVIDENCE:
- Retrofitting programs (even if not explicitly climate-focused)
- Facility upgrades that improve resilience
- Infrastructure adaptation investments
- Building improvements that address weather/climate
- Asset hardening or protection programs

SCORING GUIDANCE:
- Score 1-2: Generic facility maintenance without adaptation focus
- Score 3: Retrofitting program covering ≥25% of vulnerable assets
- Score 4: Comprehensive program covering ≥50% with quantified investment
- Score 5: Group-wide program (≥75%) with outcomes tracking
"""
    },
    
    # ===================================================================
    # CATEGORY 4: Crisis Management (M22-M26)
    # Currently: 8-12% evidence discovery
    # Target: 50-70% evidence discovery
    # ===================================================================
    "M22": {
        "name": "Business continuity plans (BCPs)",
        "search_keywords": [
            "business continuity", "BCP", "continuity plan",
            "disaster recovery", "emergency preparedness",
            "operational resilience", "continuity management"
        ],
        "acceptable_evidence": [
            "Business continuity plans (hazard-agnostic)",
            "Disaster recovery plans",
            "Emergency preparedness programs",
            "Operational resilience frameworks",
            "Continuity management systems"
        ],
        "extraction_guidance": """
HAZARD-AGNOSTIC MEASURE: Accept evidence for ANY major disruption, not just climate-specific.

SEARCH STRATEGY:
- Look for "business continuity plan", "BCP", "disaster recovery"
- Look for "emergency preparedness", "operational resilience"
- Look for "continuity management", "incident response"

ACCEPTABLE EVIDENCE:
- Business continuity plans (any hazard)
- Disaster recovery plans (any disaster)
- Emergency preparedness programs
- Operational resilience frameworks
- ISO 22301 certification (business continuity)

SCORING GUIDANCE:
- Score 1-2: Generic mention of continuity without documentation
- Score 3: Documented BCP covering ≥50% of critical operations
- Score 4: Comprehensive BCP covering ≥80% with testing
- Score 5: Group-wide BCP (≥95%) with regular testing and continuous improvement
"""
    },
    
    "M24": {
        "name": "Crisis communication systems",
        "search_keywords": [
            "crisis communication", "emergency communication",
            "incident communication", "stakeholder communication",
            "emergency notification", "alert system"
        ],
        "acceptable_evidence": [
            "Crisis communication systems (hazard-agnostic)",
            "Emergency notification systems",
            "Stakeholder communication protocols",
            "Incident communication procedures",
            "Alert and warning systems"
        ],
        "extraction_guidance": """
HAZARD-AGNOSTIC MEASURE: Accept evidence for ANY crisis communication, not just climate-specific.

SEARCH STRATEGY:
- Look for "crisis communication", "emergency communication"
- Look for "incident notification", "alert system"
- Look for "stakeholder communication during emergencies"

ACCEPTABLE EVIDENCE:
- Crisis communication systems (any crisis)
- Emergency notification systems
- Stakeholder communication protocols
- Incident communication procedures
- Employee/customer alert systems

SCORING GUIDANCE:
- Score 1-2: Generic mention of communication without systems
- Score 3: Documented system covering ≥3 stakeholder groups
- Score 4: Comprehensive system covering ≥5 groups with testing
- Score 5: Group-wide system with multiple channels and performance tracking
"""
    },
    
    "M25": {
        "name": "Recovery time objectives (RTOs)",
        "search_keywords": [
            "recovery time objective", "RTO", "RPO",
            "recovery targets", "restoration time",
            "downtime targets", "recovery metrics"
        ],
        "acceptable_evidence": [
            "Recovery time objectives (hazard-agnostic)",
            "Restoration time targets",
            "Downtime objectives",
            "Recovery metrics",
            "Business continuity metrics"
        ],
        "extraction_guidance": """
HAZARD-AGNOSTIC MEASURE: Accept evidence for ANY disruption, not just climate-specific.

SEARCH STRATEGY:
- Look for "recovery time objective", "RTO", "RPO"
- Look for "recovery targets", "restoration time"
- Look for "downtime targets", "recovery metrics"

ACCEPTABLE EVIDENCE:
- Defined RTOs for critical operations (any disruption)
- Recovery targets with specific time objectives
- Downtime targets and metrics
- Business continuity metrics
- Criticality classification with recovery times

SCORING GUIDANCE:
- Score 1-2: Generic mention of recovery without objectives
- Score 3: Defined RTOs for ≥50% of critical operations
- Score 4: Comprehensive RTOs for ≥80% with achievement tracking
- Score 5: Group-wide RTOs (≥95%) with performance monitoring
"""
    },
    
    "M26": {
        "name": "Post-event review & continuous improvement",
        "search_keywords": [
            "post-event review", "lessons learned",
            "after-action review", "incident review",
            "continuous improvement", "post-mortem"
        ],
        "acceptable_evidence": [
            "Post-event review processes (hazard-agnostic)",
            "Lessons learned programs",
            "After-action reviews",
            "Incident review procedures",
            "Continuous improvement processes"
        ],
        "extraction_guidance": """
HAZARD-AGNOSTIC MEASURE: Accept evidence for ANY incident review, not just climate-specific.

SEARCH STRATEGY:
- Look for "post-event review", "lessons learned", "after-action"
- Look for "incident review", "post-mortem", "continuous improvement"
- Look for "root cause analysis", "corrective actions"

ACCEPTABLE EVIDENCE:
- Post-event review processes (any event)
- Lessons learned documentation
- After-action review procedures
- Incident review and analysis
- Continuous improvement programs

SCORING GUIDANCE:
- Score 1-2: Generic mention of reviews without process
- Score 3: Documented process reviewing ≥50% of significant events
- Score 4: Comprehensive process with improvements tracked
- Score 5: Group-wide process with organizational learning and accountability
"""
    },
    
    # ===================================================================
    # CATEGORY 5: Supply Chain Management (M27-M31)
    # Currently: 8% evidence discovery
    # Target: 30-50% evidence discovery
    # ===================================================================
    "M27": {
        "name": "Supplier risk assessment & mapping",
        "search_keywords": [
            "supplier risk assessment", "supply chain risk",
            "supplier mapping", "supplier assessment",
            "supply chain vulnerability", "supplier audit"
        ],
        "acceptable_evidence": [
            "Supplier risk assessments",
            "Supply chain risk mapping",
            "Supplier vulnerability assessments",
            "Geographic supplier mapping",
            "Supplier climate risk analysis"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "supplier risk assessment", "supply chain risk"
- Look for "supplier mapping", "supplier vulnerability"
- Look for "supplier audits", "supply chain resilience"

ACCEPTABLE EVIDENCE:
- Supplier risk assessments (even if not climate-specific)
- Supply chain risk mapping
- Supplier vulnerability assessments
- Geographic supplier mapping
- Supplier audits that include risk factors

SCORING GUIDANCE:
- Score 1-2: Generic mention of supplier management without risk assessment
- Score 3: Risk assessment covering ≥50% of suppliers by spend
- Score 4: Comprehensive assessment covering ≥80% with multiple hazards
- Score 5: Group-wide assessment (≥95%) with regular updates
"""
    },
    
    "M28": {
        "name": "Supplier diversification & redundancy",
        "search_keywords": [
            "supplier diversification", "dual sourcing",
            "supplier redundancy", "multiple suppliers",
            "supply chain diversification", "alternative suppliers"
        ],
        "acceptable_evidence": [
            "Supplier diversification strategies",
            "Dual/multiple sourcing programs",
            "Supplier redundancy plans",
            "Alternative supplier development",
            "Supply chain diversification"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "supplier diversification", "dual sourcing", "multiple suppliers"
- Look for "supplier redundancy", "alternative suppliers"
- Look for "supply chain diversification", "sourcing strategy"

ACCEPTABLE EVIDENCE:
- Supplier diversification strategies
- Dual or multiple sourcing for critical materials
- Supplier redundancy programs
- Alternative supplier development
- Geographic diversification of suppliers

SCORING GUIDANCE:
- Score 1-2: Generic mention of multiple suppliers without strategy
- Score 3: Diversification strategy for ≥50% of critical suppliers
- Score 4: Comprehensive strategy for ≥80% with implementation tracking
- Score 5: Group-wide strategy (≥95%) with performance monitoring
"""
    },
    
    # ===================================================================
    # CATEGORY 8: Employee Safety & Community Engagement (M38-M39)
    # Currently: 0-8% evidence discovery
    # Target: 30-50% evidence discovery
    # ===================================================================
    "M38": {
        "name": "Employee safety & wellbeing programs",
        "search_keywords": [
            "employee safety", "worker protection",
            "occupational health", "employee wellbeing",
            "workplace safety", "worker safety programs"
        ],
        "acceptable_evidence": [
            "Employee safety programs (climate events)",
            "Worker protection during extreme weather",
            "Occupational health programs",
            "Employee wellbeing initiatives",
            "Workplace safety protocols"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "employee safety", "worker protection", "occupational health"
- Look for "extreme weather safety", "heat stress programs"
- Look for "employee wellbeing", "workplace safety"

ACCEPTABLE EVIDENCE:
- Employee safety programs (any hazard, including weather)
- Worker protection protocols
- Occupational health and safety programs
- Employee wellbeing initiatives
- Workplace safety training

SCORING GUIDANCE:
- Score 1-2: Generic employee safety without climate/weather focus
- Score 3: Safety programs addressing weather/climate events
- Score 4: Comprehensive programs with training and monitoring
- Score 5: Group-wide programs with outcomes tracking and continuous improvement
"""
    },
    
    "M39": {
        "name": "Community engagement & support",
        "search_keywords": [
            "community engagement", "community support",
            "corporate social responsibility", "CSR",
            "community investment", "disaster relief",
            "community resilience", "community partnerships"
        ],
        "acceptable_evidence": [
            "Community engagement programs",
            "Community support during disasters",
            "Corporate social responsibility (CSR)",
            "Community investment programs",
            "Disaster relief and assistance",
            "Community resilience initiatives"
        ],
        "extraction_guidance": """
SEARCH STRATEGY:
- Look for "community engagement", "community support", "CSR"
- Look for "disaster relief", "community assistance"
- Look for "community resilience", "community partnerships"

ACCEPTABLE EVIDENCE:
- Community engagement programs (even if not climate-specific)
- Community support during disasters/emergencies
- Corporate social responsibility programs
- Community investment and partnerships
- Disaster relief and assistance programs
- Support for vulnerable communities

SCORING GUIDANCE:
- Score 1-2: Generic community engagement without disaster/climate focus
- Score 3: Community support programs during emergencies (≥2 programs)
- Score 4: Comprehensive support with quantified investment (≥3 programs)
- Score 5: Dedicated climate resilience support for communities with partnerships
"""
    }
}


def get_measure_guidance(measure_id: str) -> dict:
    """Get measure-specific guidance for extraction"""
    return MEASURE_SPECIFIC_GUIDANCE.get(measure_id, {})


def get_all_search_keywords() -> dict:
    """Get all search keywords organized by measure"""
    return {
        measure_id: guidance.get("search_keywords", [])
        for measure_id, guidance in MEASURE_SPECIFIC_GUIDANCE.items()
    }


def get_extraction_prompt(measure_id: str) -> str:
    """Get the extraction guidance prompt for a specific measure"""
    guidance = MEASURE_SPECIFIC_GUIDANCE.get(measure_id, {})
    return guidance.get("extraction_guidance", "")
