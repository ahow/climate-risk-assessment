"""
FastAPI Web Application for Physical Climate Risk Assessment System
Enhanced with ProcessPrompt v2.2 management and full feature parity
"""
import os
import io
import logging
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime

from app.database import Database, init_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Physical Climate Risk Assessment System")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database schema on startup"""
    try:
        init_database()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve main web interface with ProcessPrompt management"""
    
    # Get active ProcessPrompt info
    db = Database()
    try:
        active_pp = db.get_active_processprompt()
        all_pps = db.get_all_processprompts() or []
        recent_jobs = db.get_recent_jobs(limit=20) or []
    except Exception as e:
        logger.error(f"Error loading page data: {e}")
        active_pp = None
        all_pps = []
        recent_jobs = []
    
    active_pp_name = active_pp['version_name'] if active_pp else "None"
    active_pp_date = active_pp['uploaded_at'].strftime('%m/%d/%Y, %I:%M:%S %p') if active_pp else "N/A"
    active_pp_size = f"{active_pp['file_size'] / 1024:.1f} KB" if active_pp and active_pp.get('file_size') else "N/A"
    
    # Build ProcessPrompt version history HTML
    pp_history_html = ""
    for pp in all_pps:
        pp_date = pp['uploaded_at'].strftime('%m/%d/%Y, %I:%M:%S %p')
        pp_size = f"{pp['file_size'] / 1024:.1f} KB" if pp.get('file_size') else "N/A"
        pp_notes = pp.get('notes', '')
        is_active = pp['is_active']
        
        status_badge = '<span style="background: #28a745; color: white; padding: 2px 8px; border-radius: 4px; font-size: 12px; margin-left: 10px;">ACTIVE</span>' if is_active else ''
        
        pp_id = pp['id']
        pp_version = pp['version_name']
        activate_btn = '' if is_active else f'<button onclick="activateProcessPrompt({pp_id})" style="margin-right: 10px; padding: 5px 15px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">Activate</button>'
        notes_html = f'<br><small style="color: #666;">{pp_notes}</small>' if pp_notes else ''
        
        pp_history_html += f"""
        <div style="border: 1px solid #ddd; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>{pp_version}</strong>{status_badge}
                    <br>
                    <small style="color: #666;">{pp_date} • {pp_size}</small>
                    {notes_html}
                </div>
                <div>
                    {activate_btn}
                    <button onclick="downloadProcessPrompt({pp_id}, '{pp_version}')" style="padding: 5px 15px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Download</button>
                </div>
            </div>
        </div>
        """
    
    # Build recent jobs HTML
    recent_jobs_html = ""
    for job in recent_jobs:
        job_date = job['created_at'].strftime('%m/%d/%Y %I:%M %p')
        status_color = {
            'completed': '#28a745',
            'processing': '#ffc107',
            'pending': '#6c757d',
            'failed': '#dc3545'
        }.get(job['status'], '#6c757d')
        
        recent_jobs_html += f"""
        <div style="border-bottom: 1px solid #eee; padding: 10px 0;">
            <strong>{job['company_name']}</strong> ({job['isin']})<br>
            <small style="color: #666;">Job #{job['id']} - <span style="color: {status_color}; font-weight: bold;">{job['status'].upper()}</span></small>
        </div>
        """
    
    return f"""
<!DOCTYPE html>
<html>
<head>
    <title>Physical Climate Risk Assessment System</title>
    <meta charset="UTF-8">
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }}
        .header h1 {{
            font-size: 36px;
            margin-bottom: 10px;
        }}
        .header p {{
            font-size: 16px;
            opacity: 0.9;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            padding: 40px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin-bottom: 40px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            color: white;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }}
        .stat-number {{
            font-size: 42px;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .stat-label {{
            font-size: 14px;
            opacity: 0.95;
        }}
        .section {{
            margin-bottom: 40px;
        }}
        .section-title {{
            font-size: 24px;
            color: #333;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
        }}
        .section-title::before {{
            content: '';
            display: inline-block;
            width: 4px;
            height: 24px;
            background: #667eea;
            margin-right: 12px;
        }}
        .download-section {{
            background: #f8f9fa;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .download-section p {{
            color: #666;
            margin-bottom: 20px;
        }}
        .download-buttons {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }}
        .download-btn {{
            padding: 15px 25px;
            font-size: 16px;
            font-weight: 600;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s;
            text-align: center;
        }}
        .download-btn-primary {{
            background: #007bff;
            color: white;
        }}
        .download-btn-primary:hover {{
            background: #0056b3;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,123,255,0.3);
        }}
        .download-btn-secondary {{
            background: #6f42c1;
            color: white;
        }}
        .download-btn-secondary:hover {{
            background: #5a32a3;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(111,66,193,0.3);
        }}
        .form-box {{
            background: #f8f9fa;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .form-group {{
            margin-bottom: 15px;
        }}
        label {{
            display: block;
            font-weight: 600;
            margin-bottom: 5px;
            color: #333;
        }}
        input[type="text"], input[type="file"], textarea {{
            width: 100%;
            padding: 12px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
        }}
        input[type="text"]:focus, textarea:focus {{
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102,126,234,0.1);
        }}
        .checkbox-group {{
            display: flex;
            align-items: center;
            margin-top: 10px;
        }}
        .checkbox-group input[type="checkbox"] {{
            width: auto;
            margin-right: 8px;
        }}
        button[type="submit"], .btn {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px 30px;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }}
        button[type="submit"]:hover, .btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(102,126,234,0.4);
        }}
        .processprompt-info {{
            background: #e7f3ff;
            border-left: 4px solid #007bff;
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 20px;
        }}
        .processprompt-info strong {{
            color: #007bff;
        }}
        .recent-jobs {{
            max-height: 400px;
            overflow-y: auto;
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
        }}
        .alert {{
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 20px;
        }}
        .alert-success {{
            background: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
        }}
        .alert-info {{
            background: #d1ecf1;
            border: 1px solid #bee5eb;
            color: #0c5460;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌍 Physical Climate Risk Assessment System</h1>
        <p>Automated assessment using ProcessPrompt v2.2 methodology</p>
    </div>
    
    <div class="container">
        <!-- Statistics Dashboard -->
        <div class="stats">
            <div class="stat-card">
                <div class="stat-number" id="stat-companies">0</div>
                <div class="stat-label">Companies</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="stat-completed">0</div>
                <div class="stat-label">Completed</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="stat-processing">0</div>
                <div class="stat-label">Processing</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="stat-pending">0</div>
                <div class="stat-label">Pending</div>
            </div>
        </div>
        
        <!-- Download Assessment Results -->
        <div class="section">
            <h2 class="section-title">📥 Download Assessment Results</h2>
            <div class="download-section">
                <p>Export all completed assessments to a single CSV file. Choose between latest results only or full assessment history.</p>
                <div class="download-buttons">
                    <button class="download-btn download-btn-primary" onclick="downloadLatest()">
                        📊 Download Latest Results<br>
                        <small style="font-weight: normal; opacity: 0.9;">One assessment per company</small>
                    </button>
                    <button class="download-btn download-btn-secondary" onclick="downloadFull()">
                        📚 Download Full History<br>
                        <small style="font-weight: normal; opacity: 0.9;">All assessments including re-runs</small>
                    </button>
                </div>
            </div>
        </div>
        
        <!-- Submit Single Company -->
        <div class="section">
            <h2 class="section-title">📊 Submit Single Company</h2>
            <div class="form-box">
                <form id="singleForm">
                    <div class="form-group">
                        <label>Company Name *</label>
                        <input type="text" name="company" placeholder="e.g., Apple Inc." required>
                    </div>
                    <div class="form-group">
                        <label>ISIN *</label>
                        <input type="text" name="isin" placeholder="e.g., US0378331005" required>
                    </div>
                    <div class="form-group">
                        <label>Sector</label>
                        <input type="text" name="sector" placeholder="e.g., Technology">
                    </div>
                    <div class="form-group">
                        <label>Industry</label>
                        <input type="text" name="industry" placeholder="e.g., Consumer Electronics">
                    </div>
                    <div class="form-group">
                        <label>Country</label>
                        <input type="text" name="country" placeholder="e.g., United States">
                    </div>
                    <button type="submit">Start Assessment</button>
                </form>
            </div>
        </div>
        
        <!-- Upload CSV Batch -->
        <div class="section">
            <h2 class="section-title">📁 Upload CSV (Batch Assessment)</h2>
            <div class="form-box">
                <p style="color: #666; margin-bottom: 15px;">Upload a CSV file with columns: Company, ISIN, Sector, Industry, Country</p>
                <form id="batchForm">
                    <div class="form-group">
                        <input type="file" name="file" accept=".csv" required>
                    </div>
                    <button type="submit">Upload & Start Batch Assessment</button>
                </form>
            </div>
        </div>
        
        <!-- ProcessPrompt Management -->
        <div class="section">
            <h2 class="section-title">📝 ProcessPrompt Management</h2>
            
            <!-- Current Active Version -->
            <div class="processprompt-info">
                <h3 style="margin-bottom: 10px;">Current Active Version</h3>
                <p><strong>Version:</strong> {active_pp_name}</p>
                <p><strong>Uploaded:</strong> {active_pp_date}</p>
                <p><strong>Size:</strong> {active_pp_size}</p>
            </div>
            
            <!-- Upload New ProcessPrompt -->
            <div class="form-box">
                <h3 style="margin-bottom: 15px;">Upload New ProcessPrompt</h3>
                <p style="color: #666; margin-bottom: 15px;">Upload a new ProcessPrompt markdown document to update the assessment methodology. The system will automatically use the latest active version for all new assessments.</p>
                <form id="processpromptForm">
                    <div class="form-group">
                        <label>ProcessPrompt File (.md) *</label>
                        <input type="file" name="file" accept=".md" required>
                    </div>
                    <div class="form-group">
                        <label>Version Name (optional)</label>
                        <input type="text" name="version_name" placeholder="e.g., ProcessPrompt_v2.3">
                    </div>
                    <div class="form-group">
                        <label>Notes (optional)</label>
                        <textarea name="notes" rows="3" placeholder="Describe changes or improvements in this version"></textarea>
                    </div>
                    <div class="checkbox-group">
                        <input type="checkbox" name="set_active" id="set_active" checked>
                        <label for="set_active" style="margin: 0;">Set as active version immediately</label>
                    </div>
                    <button type="submit" style="margin-top: 15px;">Upload ProcessPrompt</button>
                </form>
            </div>
            
            <!-- Version History -->
            <div style="margin-top: 30px;">
                <h3 style="margin-bottom: 15px;">Version History</h3>
                <div id="pp-history">
                    {pp_history_html}
                </div>
            </div>
        </div>
        
        <!-- Recent Assessment Jobs -->
        <div class="section">
            <h2 class="section-title">📋 Recent Assessment Jobs</h2>
            <div class="recent-jobs" id="recent-jobs">
                {recent_jobs_html}
            </div>
        </div>
    </div>
    
    <script>
        // Load statistics
        async function loadStats() {{
            const response = await fetch('/api/stats');
            const stats = await response.json();
            
            document.getElementById('stat-companies').textContent = stats.companies;
            document.getElementById('stat-completed').textContent = stats.completed;
            document.getElementById('stat-processing').textContent = stats.processing;
            document.getElementById('stat-pending').textContent = stats.pending;
        }}
        
        // Submit single company
        document.getElementById('singleForm').addEventListener('submit', async (e) => {{
            e.preventDefault();
            const formData = new FormData(e.target);
            const data = Object.fromEntries(formData);
            
            const response = await fetch('/api/submit', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(data)
            }});
            
            const result = await response.json();
            alert(result.message);
            e.target.reset();
            loadStats();
            loadRecentJobs();
        }});
        
        // Submit batch CSV
        document.getElementById('batchForm').addEventListener('submit', async (e) => {{
            e.preventDefault();
            const formData = new FormData(e.target);
            
            const response = await fetch('/api/upload', {{
                method: 'POST',
                body: formData
            }});
            
            const result = await response.json();
            alert(result.message);
            e.target.reset();
            loadStats();
            loadRecentJobs();
        }});
        
        // Upload ProcessPrompt
        document.getElementById('processpromptForm').addEventListener('submit', async (e) => {{
            e.preventDefault();
            const formData = new FormData(e.target);
            
            const response = await fetch('/api/processprompt/upload', {{
                method: 'POST',
                body: formData
            }});
            
            const result = await response.json();
            alert(result.message);
            if (result.success) {{
                window.location.reload();
            }}
        }});
        
        // Download latest results
        function downloadLatest() {{
            window.location.href = '/api/download/latest';
        }}
        
        // Download full history
        function downloadFull() {{
            window.location.href = '/api/download/full';
        }}
        
        // Activate ProcessPrompt
        async function activateProcessPrompt(id) {{
            if (!confirm('Activate this ProcessPrompt version? All new assessments will use this version.')) return;
            
            const response = await fetch(`/api/processprompt/${{id}}/activate`, {{
                method: 'POST'
            }});
            
            const result = await response.json();
            alert(result.message);
            if (result.success) {{
                window.location.reload();
            }}
        }}
        
        // Download ProcessPrompt
        function downloadProcessPrompt(id, name) {{
            window.location.href = `/api/processprompt/${{id}}/download`;
        }}
        
        // Load recent jobs
        async function loadRecentJobs() {{
            const response = await fetch('/api/jobs/recent');
            const jobs = await response.json();
            
            let html = '';
            jobs.forEach(job => {{
                const statusColors = {{
                    'completed': '#28a745',
                    'processing': '#ffc107',
                    'pending': '#6c757d',
                    'failed': '#dc3545'
                }};
                const color = statusColors[job.status] || '#6c757d';
                
                html += `
                    <div style="border-bottom: 1px solid #eee; padding: 10px 0;">
                        <strong>${{job.company_name}}</strong> (${{job.isin}})<br>
                        <small style="color: #666;">Job #${{job.id}} - <span style="color: ${{color}}; font-weight: bold;">${{job.status.toUpperCase()}}</span></small>
                    </div>
                `;
            }});
            
            document.getElementById('recent-jobs').innerHTML = html;
        }}
        
        // Load on page load
        loadStats();
        loadRecentJobs();
        
        // Refresh every 10 seconds
        setInterval(() => {{
            loadStats();
            loadRecentJobs();
        }}, 10000);
    </script>
</body>
</html>
    """

# API Endpoints

@app.get("/api/stats")
async def get_stats():
    """Get system statistics"""
    db = Database()
    stats = db.get_stats()
    return stats

@app.post("/api/submit")
async def submit_single(data: dict):
    """Submit single company for assessment"""
    try:
        db = Database()
        
        company_id = db.get_or_create_company(
            name=data['company'],
            isin=data['isin'],
            sector=data.get('sector'),
            industry=data.get('industry'),
            country=data.get('country')
        )
        
        job_id = db.create_job(company_id)
        
        return {
            "success": True,
            "message": f"Assessment job #{job_id} created for {data['company']}",
            "job_id": job_id
        }
        
    except Exception as e:
        logger.error(f"Submit failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/upload")
async def upload_batch(file: UploadFile = File(...)):
    """Upload CSV batch for assessment"""
    try:
        # Read CSV
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Validate columns
        required_cols = ['Company', 'ISIN']
        if not all(col in df.columns for col in required_cols):
            raise HTTPException(
                status_code=400,
                detail=f"CSV must contain columns: {', '.join(required_cols)}"
            )
        
        db = Database()
        jobs_created = []
        
        # Process each row
        for _, row in df.iterrows():
            company_id = db.get_or_create_company(
                name=row['Company'],
                isin=row['ISIN'],
                sector=row.get('Sector'),
                industry=row.get('Industry'),
                country=row.get('Country')
            )
            
            job_id = db.create_job(company_id)
            jobs_created.append(job_id)
        
        return {
            "success": True,
            "message": f"Created {len(jobs_created)} assessment jobs",
            "jobs": jobs_created
        }
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/download/latest")
async def download_latest_results():
    """Download latest assessment for each company (one per company)"""
    try:
        db = Database()
        assessments = db.get_latest_assessments()
        
        # Convert to DataFrame
        df = pd.DataFrame(assessments)
        
        # Convert to CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=climate_assessments_latest_{datetime.now().strftime('%Y%m%d')}.csv"
            }
        )
        
    except Exception as e:
        logger.error(f"Download latest failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/download/full")
async def download_full_history():
    """Download all assessments including re-runs"""
    try:
        db = Database()
        assessments = db.get_all_assessments()
        
        # Convert to DataFrame
        df = pd.DataFrame(assessments)
        
        # Convert to CSV
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=climate_assessments_full_{datetime.now().strftime('%Y%m%d')}.csv"
            }
        )
        
    except Exception as e:
        logger.error(f"Download full failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/processprompt/upload")
async def upload_processprompt(
    file: UploadFile = File(...),
    version_name: str = Form(None),
    notes: str = Form(None),
    set_active: bool = Form(False)
):
    """Upload a new ProcessPrompt version"""
    try:
        # Read file content
        content = await file.read()
        content_str = content.decode('utf-8')
        
        # Generate version name if not provided
        if not version_name:
            version_name = f"ProcessPrompt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Upload to database
        db = Database()
        pp_id = db.upload_processprompt(
            version_name=version_name,
            content=content_str,
            notes=notes,
            set_active=set_active
        )
        
        return {
            "success": True,
            "message": f"ProcessPrompt '{version_name}' uploaded successfully",
            "id": pp_id
        }
        
    except Exception as e:
        logger.error(f"ProcessPrompt upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/processprompt/{pp_id}/activate")
async def activate_processprompt(pp_id: int):
    """Activate a ProcessPrompt version"""
    try:
        db = Database()
        db.activate_processprompt(pp_id)
        
        return {
            "success": True,
            "message": f"ProcessPrompt version activated successfully"
        }
        
    except Exception as e:
        logger.error(f"ProcessPrompt activation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/processprompt/{pp_id}/download")
async def download_processprompt(pp_id: int):
    """Download a ProcessPrompt version"""
    try:
        db = Database()
        pp = db.download_processprompt(pp_id)
        
        if not pp:
            raise HTTPException(status_code=404, detail="ProcessPrompt not found")
        
        # Return as markdown file
        return StreamingResponse(
            io.BytesIO(pp['content'].encode('utf-8')),
            media_type="text/markdown",
            headers={
                "Content-Disposition": f"attachment; filename={pp['version_name']}.md"
            }
        )
        
    except Exception as e:
        logger.error(f"ProcessPrompt download failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/recent")
async def get_recent_jobs():
    """Get recent assessment jobs"""
    try:
        db = Database()
        jobs = db.get_recent_jobs(limit=20)
        return jobs
        
    except Exception as e:
        logger.error(f"Get recent jobs failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "model": "DeepSeek V3",
        "search": "DuckDuckGo",
        "methodology": "ProcessPrompt v2.2"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
