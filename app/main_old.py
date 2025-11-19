"""
FastAPI Web Application for Climate Risk Assessment System
Provides web interface for uploading companies and viewing results
"""
import os
import io
import logging
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime

from app.database import Database, init_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Climate Risk Assessment System")

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
    """Serve main web interface"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Climate Risk Assessment System</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        .container {
            background: rgba(255, 255, 255, 0.95);
            padding: 30px;
            border-radius: 10px;
            color: #333;
        }
        h1 {
            color: #667eea;
            text-align: center;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin: 30px 0;
        }
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            color: white;
        }
        .stat-number {
            font-size: 36px;
            font-weight: bold;
        }
        .stat-label {
            font-size: 14px;
            opacity: 0.9;
        }
        .form-section {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        input[type="text"], input[type="file"] {
            width: 100%;
            padding: 10px;
            margin: 10px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        button {
            background: #667eea;
            color: white;
            padding: 12px 24px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:hover {
            background: #764ba2;
        }
        .download-btn {
            background: #28a745;
        }
        .download-btn:hover {
            background: #218838;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌍 Climate Risk Assessment System</h1>
        <p style="text-align: center; color: #666;">
            Powered by DeepSeek V3 with Company-Specific Web Search
        </p>
        
        <div class="stats" id="stats">
            <div class="stat-card">
                <div class="stat-number" id="companies">-</div>
                <div class="stat-label">Companies</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="completed">-</div>
                <div class="stat-label">Completed</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="processing">-</div>
                <div class="stat-label">Processing</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="pending">-</div>
                <div class="stat-label">Pending</div>
            </div>
        </div>
        
        <div class="form-section">
            <h2>📊 Submit Single Company</h2>
            <form id="singleForm">
                <input type="text" name="company" placeholder="Company Name *" required>
                <input type="text" name="isin" placeholder="ISIN *" required>
                <input type="text" name="sector" placeholder="Sector">
                <input type="text" name="industry" placeholder="Industry">
                <input type="text" name="country" placeholder="Country">
                <button type="submit">Start Assessment</button>
            </form>
        </div>
        
        <div class="form-section">
            <h2>📁 Upload CSV (Batch Assessment)</h2>
            <p>Upload a CSV file with columns: Company, ISIN, Sector, Industry, Country</p>
            <form id="batchForm">
                <input type="file" name="file" accept=".csv" required>
                <button type="submit">Upload & Start Batch Assessment</button>
            </form>
        </div>
        
        <div class="form-section">
            <h2>📥 Download Results</h2>
            <button class="download-btn" onclick="downloadResults()">📊 Download All Assessments (CSV)</button>
        </div>
    </div>
    
    <script>
        // Load stats
        async function loadStats() {
            const response = await fetch('/api/stats');
            const stats = await response.json();
            document.getElementById('companies').textContent = stats.companies;
            document.getElementById('completed').textContent = stats.completed;
            document.getElementById('processing').textContent = stats.processing;
            document.getElementById('pending').textContent = stats.pending;
        }
        
        // Submit single company
        document.getElementById('singleForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const formData = new FormData(e.target);
            const data = Object.fromEntries(formData);
            
            const response = await fetch('/api/submit', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            alert(result.message);
            e.target.reset();
            loadStats();
        });
        
        // Submit batch CSV
        document.getElementById('batchForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const formData = new FormData(e.target);
            
            const response = await fetch('/api/upload', {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            alert(result.message);
            e.target.reset();
            loadStats();
        });
        
        // Download results
        async function downloadResults() {
            window.location.href = '/api/download';
        }
        
        // Load stats on page load
        loadStats();
        
        // Refresh stats every 10 seconds
        setInterval(loadStats, 10000);
    </script>
</body>
</html>
    """

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

@app.get("/api/download")
async def download_results():
    """Download all assessment results as CSV"""
    try:
        db = Database()
        conn = db.get_connection()
        
        try:
            # Query all assessments with company info
            query = """
                SELECT 
                    c.name as company,
                    c.isin,
                    c.sector,
                    c.industry,
                    c.country,
                    a.overall_risk_rating,
                    a.physical_risk_score,
                    a.transition_risk_score,
                    a.created_at
                FROM assessments a
                JOIN companies c ON a.company_id = c.id
                ORDER BY a.created_at DESC
            """
            
            df = pd.read_sql(query, conn)
            
            # Convert to CSV
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)
            
            return StreamingResponse(
                io.BytesIO(output.getvalue().encode()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=climate_assessments_{datetime.now().strftime('%Y%m%d')}.csv"
                }
            )
            
        finally:
            db.release_connection(conn)
            
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "model": "DeepSeek V3", "search": "DuckDuckGo"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
