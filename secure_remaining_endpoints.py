"""
Script to add JWT authentication to remaining People & Operations Engine endpoints
"""

import re

file_path = r"C:\Users\tkrot\Desktop\BACKABLE AI NEW INFRASTRUCTURE DEPLOYMENTS\BACKABLE NEW PEOPLE AND OPERATION ENGINE\BACKABLE NEW INFRASTRUCTURE PEOPLE AND OPERATION ENGINE.py"

# Read the file
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Define replacements for each endpoint
replacements = [
    # /people_ops_report_status/{report_id} - just add auth, no permission check
    (
        r'@app\.get\("/people_ops_report_status/\{report_id\}"\)\nasync def get_people_ops_report_status\(report_id: str\):',
        '@app.get("/people_ops_report_status/{report_id}")\nasync def get_people_ops_report_status(report_id: str, auth: Dict = Depends(verify_jwt_token)):'
    ),

    # /people_ops_assessment_progress - POST with permission check
    (
        r'@app\.post\("/people_ops_assessment_progress"\)\nasync def save_people_ops_progress\(request: PeopleOpsProgressRequest\):',
        '@app.post("/people_ops_assessment_progress")\nasync def save_people_ops_progress(request: PeopleOpsProgressRequest, auth: Dict = Depends(verify_jwt_token)):'
    ),

    # Add permission check after function signature for save_people_ops_progress
    (
        r'(@app\.post\("/people_ops_assessment_progress"\)\nasync def save_people_ops_progress\(request: PeopleOpsProgressRequest, auth: Dict = Depends\(verify_jwt_token\)\):\s+""".*?"""\s+)(try:)',
        r'\1# Permission check: user can only save their own progress\n    if int(request.user_id) != auth["user_id"]:\n        raise HTTPException(\n            status_code=status.HTTP_403_FORBIDDEN,\n            detail="You can only save your own progress"\n        )\n\n    \2'
    ),

    # /people_ops_assessment_progress/{user_id} - GET with permission check
    (
        r'@app\.get\("/people_ops_assessment_progress/\{user_id\}"\)\nasync def get_people_ops_progress\(user_id: str\):',
        '@app.get("/people_ops_assessment_progress/{user_id}")\nasync def get_people_ops_progress(user_id: str, auth: Dict = Depends(verify_jwt_token)):'
    ),

    # Add permission check after function signature for get_people_ops_progress
    (
        r'(@app\.get\("/people_ops_assessment_progress/\{user_id\}"\)\nasync def get_people_ops_progress\(user_id: str, auth: Dict = Depends\(verify_jwt_token\)\):\s+""".*?"""\s+)(conn = None)',
        r'\1# Permission check: user can only access their own progress\n    if int(user_id) != auth["user_id"]:\n        raise HTTPException(\n            status_code=status.HTTP_403_FORBIDDEN,\n            detail="You can only access your own progress"\n        )\n\n    \2'
    ),

    # /people_ops_reports/{user_id} - GET with permission check
    (
        r'@app\.get\("/people_ops_reports/\{user_id\}"\)\nasync def get_user_people_ops_reports\(user_id: str\):',
        '@app.get("/people_ops_reports/{user_id}")\nasync def get_user_people_ops_reports(user_id: str, auth: Dict = Depends(verify_jwt_token)):'
    ),

    # Add permission check after function signature for get_user_people_ops_reports
    (
        r'(@app\.get\("/people_ops_reports/\{user_id\}"\)\nasync def get_user_people_ops_reports\(user_id: str, auth: Dict = Depends\(verify_jwt_token\)\):\s+""".*?"""\s+)(conn = None)',
        r'\1# Permission check: user can only access their own reports\n    if int(user_id) != auth["user_id"]:\n        raise HTTPException(\n            status_code=status.HTTP_403_FORBIDDEN,\n            detail="You can only access your own reports"\n        )\n\n    \2'
    ),

    # /extract_qa_data/{user_id} - GET with permission check
    (
        r'@app\.get\("/extract_qa_data/\{user_id\}"\)\nasync def extract_qa_data\(user_id: str\):',
        '@app.get("/extract_qa_data/{user_id}")\nasync def extract_qa_data(user_id: str, auth: Dict = Depends(verify_jwt_token)):'
    ),

    # Add permission check after function signature for extract_qa_data
    (
        r'(@app\.get\("/extract_qa_data/\{user_id\}"\)\nasync def extract_qa_data\(user_id: str, auth: Dict = Depends\(verify_jwt_token\)\):\s+""".*?"""\s+)(logging\.info)',
        r'\1# Permission check: user can only access their own data\n    if int(user_id) != auth["user_id"]:\n        raise HTTPException(\n            status_code=status.HTTP_403_FORBIDDEN,\n            detail="You can only access your own data"\n        )\n\n    \2'
    ),
]

# Apply each replacement
for pattern, replacement in replacements:
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)

# Write the file back
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ All endpoints secured with JWT authentication!")
