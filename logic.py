import pandas as pd
import math
import re
from datetime import datetime

DATE_FORMAT_CONFIG = {
    'formats': [
        # Existing
        '%d/%m/%Y %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%m-%d-%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',

        # Added: handle ISO 'T' and fractional seconds (dot)
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d/%m/%Y %H:%M',
        '%m/%d/%Y %H:%M',
        '%Y/%m/%d %H:%M',
        '%d-%m-%Y %H:%M',
        '%m-%d-%Y %H:%M',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y/%m/%d %H:%M:%S.%f',
        '%d-%m-%Y %H:%M:%S.%f',
        '%d/%m/%Y %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%f',
    ],
    'patterns': [
        # Existing (kept)
        r'\b\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}\b',
        r'\b\d{1,2}-\d{1,2}-\d{4} \d{1,2}:\d{2}:\d{2}\b',

        # New: YYYY-MM-DD or YYYY/MM/DD (or with T), with seconds, optional .fraction
        # Stops before " - ...", end-of-line, or a comma (so "...,454" is excluded)
        r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}[ T]\d{1,2}:\d{2}:\d{2}(?:\.\d+)?(?=(?:\s*(?:[-–—]|$))|,)',

        # New: YYYY-MM-DD or YYYY/MM/DD (or with T), minutes only
        r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}[ T]\d{1,2}:\d{2}(?=(?:\s*(?:[-–—]|$))|,)',

        # New: DMY/MDY with slash/dash/dot date sep, optional comma before time, optional .fraction
        r'\b\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4}[ ,T]?\s*\d{1,2}:\d{2}:\d{2}(?:\.\d+)?(?=(?:\s*(?:[-–—]|$))|,)',

        # New: DMY/MDY minutes only
        r'\b\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4}[ ,T]?\s*\d{1,2}:\d{2}(?=(?:\s*(?:[-–—]|$))|,)',
    ]
}


def safe_parse_timestamp(timestamp_str):
    """
    Safely parse timestamp with multiple format fallbacks.
    Returns datetime object if successful, None if all formats fail.
    """
    if not timestamp_str or pd.isna(timestamp_str):
        return None
        
    timestamp_str = str(timestamp_str).strip()
    
    for fmt in DATE_FORMAT_CONFIG['formats']:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # If all formats fail, log the issue (but don't crash)
    print(f"Warning: Could not parse timestamp '{timestamp_str}' with any known format")
    return None

def extract_timestamps_safely(text):
    """
    Extract timestamps from text using multiple patterns and formats.
    Returns list of datetime objects.
    """
    if not text or pd.isna(text):
        return []
    
    text = str(text)
    timestamps = []
    
    # Try all patterns to find potential timestamps
    for pattern in DATE_FORMAT_CONFIG['patterns']:
        matches = re.findall(pattern, text)
        for match in matches:
            parsed_dt = safe_parse_timestamp(match)
            if parsed_dt:
                timestamps.append(parsed_dt)
    
    return timestamps

def suggest_similar_columns(input_df, target_column):
    """
    Suggest similar column names that might be used instead of the target column.
    """
    available_columns = input_df.columns.tolist()
    suggestions = []
    
    # Define mapping of target columns to possible alternatives
    column_alternatives = {
        "Short description": ["Summary", "Title", "Brief Description", "Issue Summary", "Short Summary", "Description", "Subject", "Brief", "Issue Title", "Problem Summary"],
        "Description": ["Long Description", "Details", "Full Description", "Issue Description", "Problem Description", "Detailed Description"],
        "Comments and Work notes": ["Work Notes", "Comments", "Notes", "Work_Notes", "Worknotes", "Technical Notes", "Resolution Notes"],
        "Additional comments": ["Resolution Notes", "Additional Notes", "Resolution Comments", "Final Comments", "Closure Notes", "Resolution Details"],
        "Response Time": ["Response_Time", "First Response Time", "Response Time (min)", "Response Time Min"],
        "Response SLA": ["Response_SLA", "Response SLA Met", "SLA Response", "First Response SLA"],
        "Resolution SLA": ["Resolution_SLA", "Resolution SLA Met", "SLA Resolution", "Final SLA"],
        "Age": ["Ticket Age", "Days Open", "Aging", "Time Open", "Days Since Created"],
        "Knowledge Article Used": ["KBA", "KB Article", "Knowledge Base", "KB Used", "Article", "Knowledge Article"],
        "Reopened": ["Reopen Status", "Flag Reopened"],
        "Pending reason": ["Pending Reason", "Pending_Reason", "Wait Reason", "Hold Reason", "Pending Comments"],
        "Related Record": ["Related Records", "Related_Record", "Parent Record", "Related Tickets", "Related INC"],
        "Reassignment count": ["Reassignment Count", "Reassignment_Count", "Assignment Changes", "Reassignments", "Transfer Count"],
        "Priority": ["Ticket Priority", "Issue Priority", "Severity", "Urgency Level", "Priority Level"]
    }
    
    if target_column in column_alternatives:
        possible_matches = column_alternatives[target_column]
        for col in available_columns:
            for possible in possible_matches:
                if possible.lower() in col.lower() or col.lower() in possible.lower():
                    suggestions.append(col)
                    break
    
    return list(set(suggestions))  # Remove duplicates

def process_uploaded_file(input_df, selected_rules, thresholds, weights):


    sort_len_obs = ""
    long_desc_obs = ""
    response_time_obs = ""
    worknote_obs = ""
    resolution_notes_obs = ""

    import re
    import pandas as pd
    print(selected_rules, thresholds)
    output_df = pd.DataFrame()

    score_list = []
    score_category = []
 
    # Always keep identifier columns
    output_df["Ticket Number"] = input_df.get("Number", "")
    
    # if "Assigned to" in selected_rules:
    #     required_column = "Assigned to"
    #     if required_column not in input_df.columns:
    #         print(f"❌ WARNING: Missing column '{required_column}' for Assigned to validation")
    #         output_df["Assigned to Validation"] = ["Fail - Missing Column"] * len(input_df)
    #     else:
    #         output_df["Assigned to"] = input_df.get("Assigned to", "")
    output_df["Assigned to"] = input_df.get("Assigned to", "")
    # if "Assigned to" not in output_df.columns and "Assigned to" in input_df.columns:
    #     output_df["Assigned to"] = input_df["Assigned to"]
   
    output_df["Application"] = input_df.get("Application Name / CI", "")

    # output_df["Observations1"] = input_df.get("Observations1", "")      
    # output_df["Observations2"] = input_df.get("Observations2", "")
    total_checks = len(selected_rules)
 
    # Initialize result storage
    pass_matrix = []
    if 'Tower' in selected_rules:
        # Tower Dependency Validation in logic layer
        print("🏗️ Processing Tower mapping with dependency validation...")
        
        # Dependency Check 1: Input file column validation
        required_column = "Application Name / CI"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Tower logic requires '{required_column}' column in input file")
            print(f"📋 Available columns: {list(input_df.columns)}")
            print("💡 Creating Tower column with 'Missing Column' values")
            output_df['Tower'] = ["Missing Column"] * len(output_df)
        else:
            # Dependency Check 2: Tower mapping file validation
            tower_mapping_file = 'Tower_Maping.xlsx'
            try:
                tower = pd.read_excel(tower_mapping_file)
                
                # Dependency Check 3: Tower file structure validation
                required_tower_columns = ['Application Name', 'Tower']
                missing_tower_columns = [col for col in required_tower_columns if col not in tower.columns]
                if missing_tower_columns:
                    print(f"❌ WARNING: Tower mapping file missing columns: {missing_tower_columns}")
                    print(f"📋 Available columns in Tower file: {list(tower.columns)}")
                    print("💡 Creating Tower column with 'Invalid File' values")
                    output_df['Tower'] = ["Invalid File"] * len(output_df)
                else:
                    # Process tower mapping (original logic with enhanced error handling)
                    L = list()
                    tower = tower[['Application Name','Tower']]
                    
                    # Handle tower mapping with error handling
                    for i in output_df['Application']:
                        try:
                            ind = tower.loc[tower['Application Name']==i].index
                            if len(ind) > 0:  # Check if application found in tower mapping
                                tower_name = tower.loc[ind,'Tower']
                                for j in tower_name:
                                    L.append(j)
                                    break  # Take only the first match
                            else:
                                # Application not found in tower mapping, assign default
                                L.append("Unknown Tower")
                                print(f"⚠️ Warning: Application '{i}' not found in tower mapping file")
                        except Exception as e:
                            # Handle any other errors in tower mapping
                            L.append("Unknown Tower")
                            print(f"⚠️ Error mapping tower for application '{i}': {str(e)}")
                    
                    # Ensure L has the same length as the DataFrame
                    if len(L) != len(output_df):
                        print(f"⚠️ Warning: Tower mapping length mismatch. Expected {len(output_df)}, got {len(L)}")
                        # Pad with "Unknown Tower" if needed
                        while len(L) < len(output_df):
                            L.append("Unknown Tower")
                        # Trim if too long
                        L = L[:len(output_df)]
                            
                    output_df['Tower'] = L
                    print(f"✅ Tower mapping completed successfully. Unique towers found: {output_df['Tower'].nunique()}")
                    
            except FileNotFoundError:
                print(f"❌ WARNING: Tower mapping file '{tower_mapping_file}' not found")
                print("💡 Creating Tower column with 'File Missing' values")
                output_df['Tower'] = ["File Missing"] * len(output_df)
            except Exception as e:
                print(f"❌ WARNING: Cannot read tower mapping file: {str(e)}")
                print("💡 Creating Tower column with 'File Error' values")
                output_df['Tower'] = ["File Error"] * len(output_df)
        pass_matrix.append(L)


    #Short Description
    if "Short Description Length Check" in selected_rules:
        required_column = "Short description"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Short Description validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            # Create result with failure reason in the result itself
            result = ["Fail - Missing Column"] * len(input_df)
            output_df["Short Description"] = result
            sort_len_obs = "Short Description"
            pass_matrix.append(["Fail"] * len(input_df))  # Pass matrix needs simple Fail/Pass for scoring
        else:
            result = []
            min_len = thresholds["short_desc"]
            
            for val in input_df[required_column]:
                if pd.isnull(val) or len(str(val).strip()) == 0:
                    result.append("Fail")
                elif len(str(val).replace(" ", "")) < min_len:
                    result.append("Fail")
                else:
                    result.append("Pass")
            
            output_df["Short Description"] = result
            sort_len_obs = "Short Description"
            pass_matrix.append(result)
 
    # Long Description
    if "Long Description Length Check" in selected_rules:
        required_column = "Description"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Long Description validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            # Create result with failure reason in the result itself
            result = ["Fail - Missing Column"] * len(input_df)
            output_df["Long Description"] = result
            long_desc_obs = "Long Description"
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            result = []
            min_len = thresholds["long_desc"]
            
            for val in input_df[required_column]:
                if pd.isnull(val) or len(str(val).strip()) == 0:
                    result.append("Fail")
                elif len(str(val).replace(" ", "")) < min_len:
                    result.append("Fail")
                else:
                    result.append("Pass")
            
            output_df["Long Description"] = result
            long_desc_obs = "Long Description"
            pass_matrix.append(result)
 
    # Response Time (Priority-Based)
    if "Actual Response Time took" in selected_rules:
        required_column = "Response Time"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Response Time validation")

            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")

            print("💡 All entries will FAIL this validation")

            result = ["Fail - Missing Column"] * len(input_df)
            output_df["Response Time"] = result
            response_time_obs = "Response Time"
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            result = []
            
            # Define priority canonicalization function
            def normalize_priority_for_response(priority_str):
                """Normalize priority to standard format: 1-Critical, 2-High, 3-Medium, 4-Low"""
                if pd.isnull(priority_str) or str(priority_str).strip() == 'None':
                    return None
                
                key = str(priority_str).strip().lower()
                key = key.replace(' – ', '-').replace(' - ', '-').replace('–', '-')
                key = ' '.join(key.split())
                
                PRIORITY_CANON_MAP = {
                    '1-critical': '1-Critical', 'critical': '1-Critical', 'p1': '1-Critical',
                    'severity 1': '1-Critical', 'sev1': '1-Critical', 's1': '1-Critical', 'priority 1': '1-Critical',
                    '2-high': '2-High', 'high': '2-High', 'p2': '2-High',
                    'severity 2': '2-High', 'sev2': '2-High', 's2': '2-High', 'priority 2': '2-High',
                    '3-medium': '3-Medium', '3-moderate': '3-Medium', 'moderate': '3-Medium', 'med': '3-Medium', 'mod': '3-Medium',
                    'p3': '3-Medium', 'severity 3': '3-Medium', 'sev3': '3-Medium', 's3': '3-Medium', 'priority 3': '3-Medium',
                    '4-low': '4-Low', 'low': '4-Low', 'p4': '4-Low',
                    'severity 4': '4-Low', 'sev4': '4-Low', 's4': '4-Low', 'priority 4': '4-Low',
                }
                
                if key in PRIORITY_CANON_MAP:
                    return PRIORITY_CANON_MAP[key]
                
                for pat in (r'\bp\s*-?\s*([1-4])\b', r'\bsev(?:erity)?\s*-?\s*([1-4])\b', 
                           r'\bs\s*-?\s*([1-4])\b', r'\bpriority\s*-?\s*([1-4])\b'):
                    m = re.search(pat, key)
                    if m:
                        n = int(m.group(1))
                        return ['1-Critical','2-High','3-Medium','4-Low'][n-1]
                
                m = re.search(r'\b([1-4])\s*-\s*(critical|high|medium|moderate|low)\b', key)
                if m:
                    word = m.group(2)
                    if 'critical' in word: return '1-Critical'
                    if 'high' in word:     return '2-High'
                    if 'medium' in word or 'moderate' in word: return '3-Medium'
                    if 'low' in word:      return '4-Low'
                
                if 'critical' in key: return '1-Critical'
                if 'high' in key:     return '2-High'
                if 'medium' in key or 'moderate' in key: return '3-Medium'
                if 'low' in key:      return '4-Low'
                
                return None
            
            # Check if priority-based thresholds are configured (from UI after file upload)
            has_priority_config = "response_time_by_priority" in thresholds and thresholds["response_time_by_priority"]
            
            # Check if Priority column exists
            has_priority_column = "Priority" in input_df.columns
            
            if has_priority_config and has_priority_column:
                # Priority-based response time validation
                print("✅ Using Priority-based Response Time Thresholds")
                priority_thresholds = thresholds["response_time_by_priority"]
                
                for idx, (response_time, priority) in enumerate(zip(input_df["Response Time"], input_df["Priority"])):
                    # Normalize the priority from the data
                    normalized_priority = normalize_priority_for_response(priority)
                    
                    if pd.isnull(response_time) or not isinstance(response_time, (int, float)):
                        result.append("Fail")
                    elif normalized_priority and normalized_priority in priority_thresholds:
                        threshold = priority_thresholds[normalized_priority]
                        if response_time > threshold:
                            result.append(f"Fail (>{threshold}min for {normalized_priority})")
                        else:
                            result.append("Pass")
                    else:
                        # Fallback if priority not recognized
                        default_threshold = priority_thresholds.get('2-High', 30)
                        if response_time > default_threshold:
                            result.append(f"Fail (>{default_threshold}min)")
                        else:
                            result.append("Pass")
            else:
                # Fallback to old single threshold if no priority config or no Priority column
                fallback_threshold = thresholds.get("response_time", 10)
                print(f"⚠️ Using fallback Response Time Threshold: {fallback_threshold} minutes")
                
                for response_time in input_df["Response Time"]:
                    if pd.isnull(response_time) or not isinstance(response_time, (int, float)) or response_time > fallback_threshold:
                        result.append("Fail")
                    else:
                        result.append("Pass")
            
            output_df["Response Time"] = result
            response_time_obs = "Response Time"
            pass_matrix.append(result)

    #Response SLA
    if "Response SLA Met ?" in selected_rules:
        required_column = "Response SLA"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Response SLA validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Response SLA Met'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:            
            status_response_sla = list()
            for response in input_df['Response SLA']:
                if pd.isnull(response) or response is None:
                    status_response_sla.append('Fail')
                    continue
                resp_str = str(response).strip().lower()
                if resp_str in ['breached', 'true','missed']:
                    status_response_sla.append('Fail')
                else:
                    status_response_sla.append('Pass')
            
            output_df['Response SLA Met'] = status_response_sla
            pass_matrix.append(status_response_sla)

    # Resolution SLA Met
    import re
    if "Resolution SLA Met ?" in selected_rules:
        required_column = "Resolution SLA"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Resolution SLA validation")

            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")

            print("💡 All entries will FAIL this validation")

            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Resolution SLA Met'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_resolution_sla = list()
            for response in input_df['Resolution SLA']:
                if pd.isnull(response) or response is None:
                    status_resolution_sla.append('Fail')
                    continue
                resp_str = str(response).strip().lower()
                if resp_str in ['breached', 'true','missed']:
                    status_resolution_sla.append('Fail')
                else:
                    status_resolution_sla.append('Pass')
            output_df['Resolution SLA Met'] = status_resolution_sla
            pass_matrix.append(status_resolution_sla)
   
    # KBA Tagged
    if "KBA Tagged?" in selected_rules:
        required_column = "Knowledge Article Used"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for KBA Tagged validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['KBA Tagged?'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_kba_tagged = []
            for kba1 in input_df[required_column]:
                kba1_str = str(kba1).upper() if pd.notnull(kba1) else ''       
                if 'KB' in kba1_str or 'True' in kba1_str or 'TRUE' in kba1_str:
                    status_kba_tagged.append('Pass')
                else:
                    status_kba_tagged.append('Fail')
            output_df['KBA Tagged?'] = status_kba_tagged
            pass_matrix.append(status_kba_tagged)

    if "Reopened ?" in selected_rules:
        required_column = "Reopened"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Reopened validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Reopened?'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            import re
            import pandas as pd

            status_reopened = []

            # Canonical true/false strings we will support
            TRUE_STR = {'true', 'yes', 'y', '1'}
            FALSE_STR = {'false', 'no', 'n', '0'}

            def _clean_cell(v):
                """Normalize cell: string, strip, lowercase, collapse whitespace, remove NBSP."""
                if pd.isna(v):
                    return None
                s = str(v).replace('\xa0', ' ')  # remove non-breaking space from Excel
                s = s.strip().lower()
                # collapse multiple spaces to single
                s = re.sub(r'\s+', ' ', s)
                return s

            for reopened in input_df[required_column]:
                s = _clean_cell(reopened)

                # 1) Null/empty -> Fail
                if s is None or s == '':
                    status_reopened.append('Fail')
                    continue

                # 2) Literal "none" -> Fail (business rule)
                if s == 'none':
                    status_reopened.append('Fail')
                    continue

                # 3) Native booleans
                if isinstance(reopened, bool):
                    status_reopened.append('Fail' if reopened else 'Pass')
                    continue

                # 4) Canonical true/false strings
                if s in TRUE_STR:
                    status_reopened.append('Fail')
                    continue
                if s in FALSE_STR:
                    status_reopened.append('Pass')
                    continue

                # 5) Numeric handling (supports "0", "0.0", "2", etc.)
                #    > 0 => Fail (reopened), else Pass
                try:
                    num = float(s)
                    status_reopened.append('Fail' if num > 0 else 'Pass')
                except (ValueError, TypeError):
                    # 6) Unknown text -> Fail (conservative)
                    status_reopened.append('Fail')

            # Write results
            output_df['Reopened?'] = status_reopened
            pass_matrix.append(status_reopened)
            

    # Worknote
    import re
    if "Work notes Length Check"in selected_rules:
        required_column = "Comments and Work notes"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Work notes validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df["Work notes Length"] = result
            worknote_obs = "Work notes Length"
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_worknote = []
            worknote_len = thresholds["worknote"]
            import re
            for worknote in input_df[required_column]:
                if pd.isnull(worknote):
                    status_worknote.append('Fail')
                    continue
                if (worknote == 'None'):
                    status_worknote.append('Fail')
                    continue
                #cleaned_note = re.sub(r'(?m)^[0-3]\d[-/][01]\d[-/]\d{4}\s+\d{2}:\d{2}:\d{2}\s*-\s*.+?\s+\((?:Additional comments|Work notes)\)\s*$', '', worknote)
                #cleaned_note = re.sub(r'(?m)^(?:\d{4}[-/](?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])|(?:0[1-9]|[12]\d|3[01])[-/](?:0[1-9]|1[0-2])[-/]\d{4}|(?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12]\d|3[01])[-/]\d{4})\s+\d{2}:\d{2}:\d{2}\s*-\s*.+?\s+\((?:Additional comments|Work notes)\)\s*$', '', worknote)
                cleaned_note = re.sub(
                                    r'(?m)^\(?'
                                    r'(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4})'   # YYYY-M-D or D/M/YYYY or M/D/YYYY; -, /, .; single/zero-padded
                                    r'(?:\s+\d{1,2}:\d{2}(?::\d{2})?'                                     # time: H:MM or HH:MM or HH:MM:SS
                                    r'(?:\s*(?:AM|PM|A\.M\.|P\.M\.))?'                                    # optional AM/PM with or without dots/spaces
                                    r')?'                                                                 # whole time block optional
                                    r'(?:\s*-\s*[^(\n]+(?:\s*\([^)]*\))?)?'                               # optional: " - Name" and optional "(...)" tag
                                    r'\s*',                                                               # trailing spaces
                                    '',
                                    worknote,
                                    flags=re.IGNORECASE
                                )

                cleaned_note = cleaned_note.strip()

                word_count = len(cleaned_note.split())
                char_count = len(cleaned_note)
                has_punctuation = bool(re.search(r'[.,?!]', cleaned_note))
                starts_with_capital = cleaned_note[0].isupper() if cleaned_note else False
                
                if word_count >= 20 and char_count > worknote_len and has_punctuation and starts_with_capital:
                    status_worknote.append('Pass')
                else:
                    status_worknote.append('Fail')

            print(f"WORKNOTES: Word count: {word_count}, Char count: {char_count}, Has punctuation: {has_punctuation}, Starts with capital: {starts_with_capital}")

            output_df["Work notes Length Check"] = status_worknote
            worknote_obs = "Work notes Length Check"
            pass_matrix.append(status_worknote)

    #Additional comments / Resolution Notes
    import re
    if "Resolution Notes / Additional comment Length Check" in selected_rules:
        required_column = "Resolution notes"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Additional comments validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Resolution Notes Length'] = result
            resolution_notes_obs = 'Resolution Notes Length'
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_resolution_notes = []
            resolution_notes_length = thresholds["Resolution_notes_value"]
            for note in input_df[required_column]:
                if pd.isnull(note):
                    status_resolution_notes.append('Fail')
                    continue
                if (note == 'None'):
                    status_resolution_notes.append('Fail')
                    continue
                
                cleaned_note = re.sub(
                                        r'(?m)^\(?'
                                        r'(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{4})'   # YYYY-M-D or D/M/YYYY or M/D/YYYY; -, /, .; single/zero-padded
                                        r'(?:\s+\d{1,2}:\d{2}(?::\d{2})?'                                     # time: H:MM or HH:MM or HH:MM:SS
                                        r'(?:\s*(?:AM|PM|A\.M\.|P\.M\.))?'                                    # optional AM/PM with or without dots/spaces
                                        r')?'                                                                 # whole time block optional
                                        r'(?:\s*-\s*[^(\n]+(?:\s*\([^)]*\))?)?'                               # optional: " - Name" and optional "(...)" tag
                                        r'\s*',                                                               # trailing spaces
                                        '',
                                        note,
                                        flags=re.IGNORECASE
                                    )


                cleaned_note = cleaned_note.strip()

                word_count = len(cleaned_note.split())
                char_count = len(cleaned_note)
                has_punctuation = bool(re.search(r'[.,?!]', cleaned_note))
                starts_with_capital = cleaned_note[0].isupper() if cleaned_note else False

                if word_count >= 10 and char_count > resolution_notes_length and has_punctuation and starts_with_capital:
                    status_resolution_notes.append('Pass')
                else:
                    status_resolution_notes.append('Fail')
            print(f"Resolution Notes: Word count: {word_count}, Char count: {char_count}, Has punctuation: {has_punctuation}, Starts with capital: {starts_with_capital}")
            output_df['Resolution Notes Length'] = status_resolution_notes
            resolution_notes_obs = 'Resolution Notes Length'
            pass_matrix.append(status_resolution_notes)

    if "Right Assignment group Usage" in selected_rules:
        required_column = "Assignment Group"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' in for Assignment group check validation")

            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")

            print("💡 All entries will FAIL this validation")

            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Assignment group check'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_Assig_group_check = []
            tower = pd.read_excel('Tower_Maping.xlsx')
            # Add normalized columns to the mapping DataFrame (do this once before the loop)
            def normalize(val):
                return str(val).strip().lower() if pd.notnull(val) else ""
            tower['App_norm'] = tower['Application Name'].apply(normalize)
            tower['Group_norm'] = tower['Assignment group'].apply(normalize)
            for input_app, input_group in zip(input_df['Application Name / CI'], input_df['Assignment Group']):
                app_norm = normalize(input_app)
                group_norm = normalize(input_group)
                # Find all mapping rows with matching normalized Application Name
                matching_rows = tower[tower['App_norm'] == app_norm]
                # Get all possible assignment groups for this app
                possible_groups = matching_rows['Group_norm'].tolist()
                # Debug print (optional, remove in production)
                print(f"Input: {app_norm}, {group_norm} | Mapping: {possible_groups}")
                if group_norm in possible_groups:
                    status_Assig_group_check.append('Pass')
                else:
                    status_Assig_group_check.append('Fail')
            output_df['Assignment group check'] = status_Assig_group_check
            pass_matrix.append(status_Assig_group_check)
    
    def Pending_Justification():
        status_pending_justification = []
        for val1, comment1, comment2 in zip(input_df['Pending reason'], 
        input_df['Comments and Work notes'], input_df['Additional comments']):
            # If Pending reason is null or 'None', mark as Pass
            if pd.isnull(val1) or str(val1).strip().lower() == None:
                status_pending_justification.append('Pass')
                continue

            # Combine and normalize both comment fields
            combined_text = ''
            if pd.notnull(comment1):
                combined_text += str(comment1) + ' '
            if pd.notnull(comment2):
                combined_text += str(comment2)

            combined_text = combined_text.replace(' ', '').lower()

            # Check for justification phrases
            if any(phrase in combined_text.lower() for phrase in [
                # === USER/CUSTOMER DEPENDENT ===
                'confirmationpending',
                'asperconfirmationweareclosingtheticket',
                'awaitinguserconfirmation',
                'waitingforuserreply',
                'waitingforuserinput'
                'waitingforuser',
                'waitingforuserconfirmation',
                'waitinguserinputs',
                'awaitinguserinputs',
                'waitingforuserresponse',
                'pendingwithuser',
                'awaitinguserinput',
                'awaitinguserresponse',
                'waitingforuserinput',
                'waitingforuserreply',
                'waitingonuser',
                'pendinguserinput',
                'pendinguserresponse',
                'needuserinput',
                'needuseraction',
                'waitingforcustomer',
                'awaitingcustomerresponse',
                'waitingforcustomerreply',
                'pendingwithcustomer',
                'awaitingfeedbackfromuser',
                'waitingforenduser',
                'awaitingenduserresponse',
                'waitingforuserconfirmation',
                'waitingforuserclarification',
                'usertoprovidedetails',
                'useractionrequired',
                'userresponseawaited',
                'userreplypending',
                'waitingforuseravailability',
                'scheduledusermeeting',
                'usernotavailable',
                'usertesting',
                'awaitingusertesting',
                'waitingforusertesting',
                
                # === VENDOR/THIRD PARTY ===
                'waitingforvendor',
                'pendingwithvendor',
                'awaitingvendorresponse',
                'vendorworkingonit',
                'escalatedtovendor',
                'waitingforthirdparty',
                'awaitingvendorsupport',
                'vendorapprovalrequired',
                'waitingforvendorupdate',
                'vendorticketcreated',
                'awaitingmanufacturerresponse',
                
                # === TECHNICAL/INVESTIGATION ===
                'weareworkingonit',
                'wearelookingintotheissue',
                'underinvestigation',
                'analyzingtheissue',
                'troubleshootinginprogress',
                'rootcauseanalysis',
                'technicalanalysis',
                'debugginginprogress',
                'performingtests',
                'runningdiagnostics',
                'checkinglogs',
                'monitoringthesystem',
                'awaitingtestresults',
                'testinginprogress',
                'replicatingtheissue',
                'gatheringlogs',
                'systemanalysis',
                
                # === APPROVAL/AUTHORIZATION ===
                'awaitingapproval',
                'pendingapproval',
                'waitingformanagerapproval',
                'managementapprovalrequired',
                'awaitingauthorization',
                'budgetapprovalrequired',
                'securityapprovalneeded',
                'changeapprovalrequired',
                'awaitingcabreview',
                'cabapprovalrequired',
                'escalationrequired',
                
                # === SCHEDULING/MAINTENANCE ===
                'scheduledmaintenance',
                'waitingformaintenancewindow',
                'plannedoutage',
                'scheduleddowntime',
                'awaitingmaintenanceslot',
                'scheduledforimplementation',
                'waitingforchangewindow',
                'deploymentscheduled',
                'patchingscheduled',
                'upgradewindowscheduled',
                
                # === RESOURCE/TEAM DEPENDENT ===
                'awaitingsubjectmatterexpert',
                'escalatedtospecialistteam',
                'waitingforexpertassignment',
                'assignedtoseniorteam',
                'awaitingsme',
                'l2escalation',
                'l3escalation',
                'transferredtospecialistteam',
                'expertreviewrequired',
                'awaitingteamleadreview',
                
                # === DEPENDENCY/COORDINATION ===
                'waitingfordependentticket',
                'relatedticketinprogress',
                'coordinatingwithother teams',
                'dependentonparentticket',
                'blockedbyotherissue',
                'awaitingprerequisitefixes',
                'coordinationrequired',
                'multipleagentsworking',
                'crossteamcoordination',
                
                # === PROCUREMENT/DELIVERY ===
                'waitingfordelivery',
                'hardwareordered',
                'awaitingshipment',
                'procurementinprogress',
                'purchaseordersubmitted',
                'awaitinghardware',
                'waitingforreplacement parts',
                'deliveryscheduled',
                'equipmentintransit',
                
                # === ENVIRONMENT/SYSTEM DEPENDENT ===
                'systemmaintenance',
                'environmentissue',
                'networkissue',
                'serverissue',
                'applicationdown',
                'systemunavailable',
                'platformissue',
                'infrastructureissue',
                'servicedegradation',
                'performanceissue',
                
                # === COMMUNICATION/UPDATE ===
                'wewillgetbacktoyou',
                'updateswillbeprovided',
                'progressupdatetofollow',
                'statusupdatepending',
                'communicationinprogress',
                'meetingscheduled',
                'discussionrequired',
                'conferenceorganized',
                
                # === DOCUMENTATION/COMPLIANCE ===
                'documentationinprogress',
                'procedurebeingrupdated',
                'policyreviewrequired',
                'compliancecheckneeded',
                'auditinprogress',
                'documentationreviewrequired',
                'proceduralreviewneeded',
                
                # === COMMON SUPPORT PHRASES ===
                'ticketescalated',
                'furtheranalysisrequired',
                'monitoringforfurtherissues',
                'awaitingconfirmation',
                'solutionbeingtested',
                'resolutioninprogress',
                'workingonthisticket',
                'issuebeinginvestigated',
                'lookingintotheissue',
                'checkingwithbackendteam'

            ]):
                status_pending_justification.append('Pass')
            else:
                status_pending_justification.append('Fail')
        return status_pending_justification

    if "Right Pending Justification Usage" in selected_rules:
        required_columns = ["Pending reason", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Pending Justification validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Pending Justification'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            pending_justification = Pending_Justification()
            output_df['Pending Justification'] = pending_justification
            pass_matrix.append(pending_justification)

    # Related records tagged?
    """Related Records Tagged — Short Logic
            For each ticket, check if the Related Record field is correctly tagged based on the Pending reason:

            pendingchange → Related Record must contain 'CHG'
            pendingvendor → Related Record must be present (any value except null/'None')
            pendingproblem → must contain 'PRB'
            pendingfulfillment → must contain 'RITM'
            pendingincident → must contain 'INC'
            pendingcustomer → uses external justification logic (Pending_Justification())
            Remarks (possible results):

            Pass → Related record is correctly tagged for the given pending reason.
            Fail → Related record is missing or incorrectly tagged.
            Fail - Missing Column → Required columns are missing from the input.
            Pass → If Pending reason is null or 'None'."""
    
    if "Related records tagged?" in selected_rules:
        required_columns = ["Pending reason", "Related Record"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Related records tagged validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Related records tagged?'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_related_rec_tagged = []
            cnt= 0
            for record in input_df['Pending reason']:
                cnt +=1
                if pd.isnull(record):
                    status_related_rec_tagged.append('Pass')
                    continue
                if (record == 'None'):
                    status_related_rec_tagged.append('Pass')
                    continue
                record = record.replace(' ','')
                record = record.lower()
                related_rec = input_df['Related Record'].iloc[cnt-1]

                if record == 'pendingchange':
                    if pd.isnull(related_rec):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if (related_rec == 'None'):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if 'CHG' in related_rec:
                        status_related_rec_tagged.append('Pass')
                        continue
                    else:
                        status_related_rec_tagged.append('Fail')
                        continue

                elif record == 'pendingvendor':
                    
                    if pd.isnull(related_rec):
                        status_related_rec_tagged.append('Fail')
                        continue
                    elif (related_rec == 'None'):
                        status_related_rec_tagged.append('Fail')
                        continue
                    else:
                        status_related_rec_tagged.append('Pass')
                        continue

                elif record == 'pendingproblem':
                    if pd.isnull(related_rec):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if (related_rec == 'None'):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if 'PRB' in related_rec:
                        
                        status_related_rec_tagged.append('Pass')
                        continue
                    else:
                        status_related_rec_tagged.append('Fail')
                        continue

                elif record == 'pendingfulfillment':
                    
                    if pd.isnull(related_rec):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if (related_rec == 'None'):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if 'RITM' in related_rec:
                        status_related_rec_tagged.append('Pass')
                        continue
                    else:
                        status_related_rec_tagged.append('Fail')
                        continue
                    
                    # ADD pendingincident
                elif record == 'pendingincident':
                    
                    if pd.isnull(related_rec):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if (related_rec == 'None'):
                        status_related_rec_tagged.append('Fail')
                        continue
                    if 'INC' in related_rec:
                        status_related_rec_tagged.append('Pass')
                        continue
                    else:
                        status_related_rec_tagged.append('Fail')
                        continue

                elif record == 'pendingcustomer':
                    pending = Pending_Justification()
                    result= pending[cnt-1]
                    status_related_rec_tagged.append(result)

                else:
                    status_related_rec_tagged.append('Fail')
                    continue
            output_df['Related records tagged?'] = status_related_rec_tagged
            pass_matrix.append(status_related_rec_tagged)

    # Ticket Ageing Check   
    if "Ticket Ageing Check" in selected_rules:
        required_column = "Age"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Ticket Ageing Check validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Ticket Ageing Check'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_ticket_ageing_check = []
            age_threshold = thresholds.get("Age Value", 20)  # Default to 20 if not set
            
            for age in input_df[required_column]:
                if pd.isnull(age):
                    status_ticket_ageing_check.append('Fail')
                    continue
                if (age == 'None'):
                    status_ticket_ageing_check.append('Fail')
                    continue
                if age > age_threshold:
                    status_ticket_ageing_check.append('Fail')
                else:
                    status_ticket_ageing_check.append('Pass')

            output_df['Ticket Ageing Check'] = status_ticket_ageing_check
            pass_matrix.append(status_ticket_ageing_check)

    # 3_Strike rule check (Optimized with same-day closure and X-business-day handling)
    
    def three_strike_rule_check():
        """
        Optimized 3-Strike rule check for escalation policy:
        
        1. If ticket is closed within X business days → Pass (no reminders needed)
        2. If ticket opened and closed same day → Pass
        3. If Age is missing or None → Pass (missing timestamps not mandatory)
        4. If Age < 3 days → Pass (no reminder needed yet)
        5. Age 3–5 → check for first reminder
        6. Age 5–7 → check for first + second reminders  
        7. Age > 7 → check for final reminder
        """
        import re
        import numpy as np
        from datetime import datetime
        
        # Define reminder patterns
        first_reminder_patterns = [
            r'\bfirst reminder\b',
            r'\bgentle reminder\b',
            r'\b1st reminder\b',
            r'\breminder 1\b',
            r'\breminder[\s\-:#]*1\b',
            r'\breminder\b.*\b1\b',
            r'\breminder\b.*\bfirst\b',
            r'\br1\b',
            r'\breminder[\s\-]*one\b',
            # Additional patterns
            r'\binitial reminder\b',
            r'\breminder number one\b',
            r'\bfirst follow[\s\-]*up\b',
            r'\bfollow[\s\-]*up[\s\-]*1\b',
            r'\breminder sent first\b',
            r'\breminder sent on\b.*\bfirst\b'
        ]

        second_reminder_patterns = [
            r'\bsecond reminder\b',
            r'\b2nd reminder\b',
            r'\breminder 2\b',
            r'\breminder[\s\-:#]*2\b',
            r'\breminder\b.*\b2\b',
            r'\breminder\b.*\bsecond\b',
            r'\br2\b',
            r'\breminder[\s\-]*two\b',
            # Additional patterns
            r'\breminder number two\b',
            r'\bsecond follow[\s\-]*up\b',
            r'\bfollow[\s\-]*up[\s\-]*2\b',
            r'\banother reminder\b',
            r'\breminder sent second\b',
            r'\breminder again\b'
        ]

        final_reminder_patterns = [
            r'\bfinal reminder\b',
            r'\bthird reminder\b',
            r'\b3rd reminder\b',
            r'\breminder 3\b',
            r'\breminder[\s\-:#]*3\b',
            r'\breminder\b.*\b3\b',
            r'\breminder\b.*\bthird\b',
            r'\blast 3rd\b',
            r'\b3rd last\b',
            r'\bthird last\b',
            r'\br3\b',
            r'\breminder[\s\-]*three\b',
            # Additional patterns
            r'\blast reminder\b',
            r'\bfinal follow[\s\-]*up\b',
            r'\bfollow[\s\-]*up[\s\-]*3\b',
            r'\breminder number three\b',
            r'\bultimate reminder\b',
            r'\bclosing reminder\b',
            r'\breminder before closure\b',
            r'\breminder before escalation\b'
        ]

        # Get X-business-days threshold (default 3 days)
        closure_threshold_days = thresholds.get("3_strike_closure_threshold", 3)
        
        cnt = 0
        status_3_Strike_rule_check = []

        for idx, row in input_df.iterrows():
            cnt += 1
            age = row.get('Age')
            
            # Check 1: If Age is missing or None → Pass (timestamps not mandatory)
            if pd.isnull(age) or age == 'None':
                status_3_Strike_rule_check.append('Pass - Missing timestamps')
                continue
            
            # Check 2: Check if ticket is same-day closure (Age ≈ 0)
            if age <= 0.04167:  # Less than 1 hour, approximately same day
                status_3_Strike_rule_check.append('Pass - Same-day closure')
                continue
            
            # Check 3: Check if ticket closed within X business days
            if age < closure_threshold_days:
                status_3_Strike_rule_check.append(f'Pass - Closed <{closure_threshold_days}d')
                continue

            # Check 4: If Age < 3 days → Pass (no reminder needed yet)
            if age < 3:
                status_3_Strike_rule_check.append('Pass - Age <3d')
                continue

            # Get comments
            comment = input_df['Comments and Work notes'].iloc[cnt - 1]
            additional_comment = input_df['Additional comments'].iloc[cnt - 1]
            
            # Combine both comment fields
            combined_text = ''
            if pd.notnull(comment):
                combined_text += str(comment) + ' '
            if pd.notnull(additional_comment):
                combined_text += str(additional_comment)

            if not combined_text.strip():
                status_3_Strike_rule_check.append('Fail - No comments')
                continue

            # Select reminder patterns based on age
            if 3 <= age <= 5:
                reminder_patterns = first_reminder_patterns
                pattern_label = "1st reminder"
            elif 5 < age <= 7:
                reminder_patterns = first_reminder_patterns + second_reminder_patterns
                pattern_label = "1st/2nd reminder"
            else:  # age > 7
                reminder_patterns = final_reminder_patterns
                pattern_label = "final reminder"

            combined_pattern = re.compile('|'.join(reminder_patterns), re.IGNORECASE)
            matches = combined_pattern.findall(combined_text)

            if matches:
                status_3_Strike_rule_check.append(f'Pass - {pattern_label} found')
            else:
                status_3_Strike_rule_check.append(f'Fail - No {pattern_label}')
                
        return status_3_Strike_rule_check
        
    if "3 Strike rule check(escalation policy check for Remainder)" in selected_rules:
        required_columns = ["Age", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for 3 Strike rule validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['3 Strike rule remainders check'] = result
            pass_matrix.append(["Fail"] * len(input_df))

        else:    
            status_3_Strike_rule_check = three_strike_rule_check()
            output_df['3 Strike rule remainders check'] = status_3_Strike_rule_check
            pass_matrix.append(status_3_Strike_rule_check)

    # Reassignment check?
    if "Reassignment check?" in selected_rules:
        required_column = "Reassignment count"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Reassignment check validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Reassignment check?'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_reassignment_check = []
            Reassignment_check_length = thresholds.get("Reassignment threshold", 3)  # Default to 3

            for reassign in input_df[required_column]:
                if pd.isnull(reassign):
                    status_reassignment_check.append('Fail')
                    continue
                if (reassign == 'None'):
                    status_reassignment_check.append('Fail')
                    continue
                if reassign >= Reassignment_check_length:
                    status_reassignment_check.append('Fail')
                else:
                    status_reassignment_check.append('Pass')

            output_df['Reassignment check?'] = status_reassignment_check
            pass_matrix.append(status_reassignment_check)

    # Has attachment column is selected then it will return This ticket have attachment if it is true else No attachment this validation rule is dependant from input template file
    if "Has Attachments" in selected_rules:
        required_column = "Has Attachments"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Has attachment validation")
            
            # Suggest similar columns
            suggestions = suggest_similar_columns(input_df, required_column)
            if suggestions:
                print(f"💡 Similar columns: {suggestions}")
            
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Has Attachments'] = result
            #pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_has_attachment = []
            for attachment in input_df[required_column]:
                if pd.isnull(attachment):
                    status_has_attachment.append('No data found')
                    continue
                if (attachment == 'None'):
                    status_has_attachment.append('No data found')
                    continue
                attachment_str = str(attachment).upper()       
                if 'TRUE' in attachment_str or 'True' in attachment_str or 'true' in attachment_str or 'YES' in attachment_str or 'Yes' in attachment_str or 'yes' in attachment_str:
                    status_has_attachment.append('This ticket have attachment')
                else:
                    status_has_attachment.append('No attachment')

            output_df['Has Attachments'] = status_has_attachment
            #pass_matrix.append(status_has_attachment)
            
    # Priority Validation
    if "Priority Validation" in selected_rules:
        required_columns = ["Priority", "Impact", "Urgency"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Priority Validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Priority Validation'] = result
            pass_matrix.append(["Fail"] * len(input_df))

        else:            
            status_priority_val = []
            import re

            # Separate, centralized synonym maps (edit per project)
            IMPACT_URGENCY_CANON_MAP = {
                '1-high': '1-High', 'high': '1-High', 'h': '1-High', 'urgent': '1-High', 'immediate': '1-High',
                '2-medium': '2-Medium', 'medium': '2-Medium', 'moderate': '2-Medium', 'med': '2-Medium', 'mod': '2-Medium',
                'normal': '2-Medium', 'standard': '2-Medium',
                '3-low': '3-Low', 'low': '3-Low', 'l': '3-Low', 'minor': '3-Low',
            }

            PRIORITY_CANON_MAP = {
                '1-critical': '1-Critical', 'critical': '1-Critical', 'p1': '1-Critical',
                'severity 1': '1-Critical', 'sev1': '1-Critical', 's1': '1-Critical', 'priority 1': '1-Critical',

                '2-high': '2-High', 'high': '2-High', 'p2': '2-High',
                'severity 2': '2-High', 'sev2': '2-High', 's2': '2-High', 'priority 2': '2-High',

                '3-medium': '3-Medium', '3-moderate': '3-Medium', 'moderate': '3-Medium', 'med': '3-Medium', 'mod': '3-Medium',
                'p3': '3-Medium', 'severity 3': '3-Medium', 'sev3': '3-Medium', 's3': '3-Medium', 'priority 3': '3-Medium',

                '4-low': '4-Low', 'low': '4-Low', 'p4': '4-Low',
                'severity 4': '4-Low', 'sev4': '4-Low', 's4': '4-Low', 'priority 4': '4-Low',
            }

            def _canon(s, kind):
                """Canonicalize to matrix labels with robust handling of case/dash/space and regex patterns."""
                if pd.isnull(s) or s == 'None':
                    return s
                t = str(s).strip()
                t = t.replace(' – ', '-').replace(' - ', '-').replace('–', '-')
                t = ' '.join(t.split())
                key = t.lower()

                base_map = PRIORITY_CANON_MAP if kind == 'priority' else IMPACT_URGENCY_CANON_MAP
                if key in base_map:
                    return base_map[key]

                if kind == 'priority':
                    for pat in (r'\bp\s*-?\s*([1-4])\b',
                                r'\bsev(?:erity)?\s*-?\s*([1-4])\b',
                                r'\bs\s*-?\s*([1-4])\b',
                                r'\bpriority\s*-?\s*([1-4])\b'):
                        m = re.search(pat, key)
                        if m:
                            n = int(m.group(1))
                            return ['1-Critical','2-High','3-Medium','4-Low'][n-1]

                    m = re.search(r'\b([1-4])\s*-\s*(critical|high|medium|moderate|low)\b', key)
                    if m:
                        word = m.group(2)
                        if 'critical' in word: return '1-Critical'
                        if 'high' in word:     return '2-High'
                        if 'medium' in word or 'moderate' in word: return '3-Medium'
                        if 'low' in word:      return '4-Low'

                    if 'critical' in key: return '1-Critical'
                    if 'high' in key:     return '2-High'
                    if 'medium' in key or 'moderate' in key: return '3-Medium'
                    if 'low' in key:      return '4-Low'

                else:  # kind == 'iu'
                    m = re.search(r'\b([1-3])\s*-\s*(high|medium|moderate|low)\b', key)
                    if m:
                        n = int(m.group(1))
                        word = m.group(2)
                        if n == 1 or 'high' in word: return '1-High'
                        if n == 2 or 'medium' in word or 'moderate' in word: return '2-Medium'
                        if n == 3 or 'low' in word: return '3-Low'

                    if 'high' in key or 'urgent' in key or 'immediate' in key: return '1-High'
                    if 'medium' in key or 'moderate' in key or 'normal' in key or 'standard' in key: return '2-Medium'
                    if 'low' in key or 'minor' in key: return '3-Low'

                    m = re.search(r'\b([1-3])\b', key)
                    if m:
                        n = int(m.group(1))
                        return ['1-High','2-Medium','3-Low'][n-1]

                return t

            def normalize_priority_value(value):
                if pd.isnull(value) or value == 'None':
                    return value
                normalized = str(value).strip()
                normalized = normalized.replace(' – ', '-').replace(' - ', '-').replace('–', '-')
                return normalized

            def validate_priority_impact_urgency(impact, urgency, priority):
                norm_impact = normalize_priority_value(impact)
                norm_urgency = normalize_priority_value(urgency)
                norm_priority = normalize_priority_value(priority)

                valid_combinations = [
                    ("1-High", "1-High", "1-Critical"),
                    ("1-High", "2-Medium", "2-High"),
                    ("1-High", "3-Low", "3-Medium"),
                    ("2-Medium", "1-High", "2-High"),
                    ("2-Medium", "2-Medium", "3-Medium"),
                    ("2-Medium", "3-Low", "4-Low"),
                    ("3-Low", "1-High", "3-Medium"),
                    ("3-Low", "2-Medium", "4-Low"),
                    ("3-Low", "3-Low", "4-Low")
                ]
                return "Pass" if (norm_impact, norm_urgency, norm_priority) in valid_combinations else "Fail"


            # ✅ ONLY CHANGE IS HERE: safer index-based loop (no cnt / iloc[cnt-1])
            for idx, val in input_df['Priority'].items():
                if pd.isnull(val) or val == 'None':
                    status_priority_val.append('Fail')
                    continue

                impact  = input_df.at[idx, 'Impact']
                urgency = input_df.at[idx, 'Urgency']

                if pd.isnull(impact) or pd.isnull(urgency) or impact == 'None' or urgency == 'None':
                    status_priority_val.append('Fail')
                    continue

                impact_canon   = _canon(impact,  'iu')
                urgency_canon  = _canon(urgency, 'iu')
                priority_canon = _canon(val,     'priority')

                status_priority_val.append(
                    validate_priority_impact_urgency(impact_canon, urgency_canon, priority_canon)
                )

            output_df['Priority Validation'] = status_priority_val
            pass_matrix.append(status_priority_val)


    if "Category Validation" in selected_rules:
        required_columns = ["Description", "Category", "Subcategory"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Category Validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Category Validation'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        # Comment out old code
        # Updated Version
        # checking for any combination (order-insensitive token subset match)
        else:
            import re
            import pandas as pd
            # ---- Helpers (local scope, NaN-safe) ----
            def _norm_str(s):
                '''Normalize for comparison: lower, trim, remove spaces; treat '', 'None', NaN as None.'''
                if pd.isna(s):
                    return None
                s = str(s).strip()
                if s == '' or s.lower() == 'none':
                    return None
                return re.sub(r'\s+', '', s.lower())

            def _tokenize(s):
                '''Tokenize text into lowercase alphanumeric tokens; returns [] for None/NaN.'''
                if pd.isna(s):
                    return []
                s = str(s).lower()
                # Split on non-alphanumeric boundaries; adjust if underscores/hyphens need to be preserved.
                tokens = re.split(r'[^a-z0-9]+', s)
                return [t for t in tokens if t]

            # ---- Load mapping and build keyword index (preprocessed for speed) ----
            keywords = pd.read_excel("Category_Subcategory_Mapping.xlsx")
            if not {'Keywords', 'Category'}.issubset(keywords.columns):
                raise ValueError("Mapping file must contain 'Keywords' and 'Category' columns")

            keyword_index = []  # list of tuples: (kw_tokens_fset, kw_cat_norm, kw_sub_norm)
            has_sub = 'Subcategory' in keywords.columns
            for _, krow in keywords.iterrows():
                kw_tokens = frozenset(_tokenize(krow.get('Keywords')))
                if not kw_tokens:
                    # Skip empty keyword rows
                    continue
                kw_cat_norm = _norm_str(krow.get('Category'))
                kw_sub_norm = _norm_str(krow.get('Subcategory')) if has_sub else None
                keyword_index.append((kw_tokens, kw_cat_norm, kw_sub_norm))

            # Prefer the most specific match (largest token set) first
            keyword_index.sort(key=lambda t: len(t[0]), reverse=True)

            # ---- Validate each input row ----
            status_category_val = []
            has_input_sub = 'Subcategory' in input_df.columns

            for _, irow in input_df.iterrows():
                # 1) Description tokens
                desc_tokens = set(_tokenize(irow.get('Description')))
                if not desc_tokens:
                    status_category_val.append('Fail')
                    continue

                # 2) Best match where keyword tokens ⊆ description tokens
                match = None
                for kw_tokens, kw_cat_norm, kw_sub_norm in keyword_index:
                    if kw_tokens.issubset(desc_tokens):
                        match = (kw_cat_norm, kw_sub_norm)
                        break

                if match is None:
                    status_category_val.append('Fail')
                    continue

                kw_cat_norm, kw_sub_norm = match

                # 3) Category comparison (normalized)
                input_cat_norm = _norm_str(irow.get('Category'))
                if kw_cat_norm is None or input_cat_norm is None or kw_cat_norm != input_cat_norm:
                    status_category_val.append('Fail')
                    continue

                # 4) Subcategory comparison only if both sides have non-empty values
                if has_input_sub:
                    input_sub_norm = _norm_str(irow.get('Subcategory'))
                    if kw_sub_norm is not None and input_sub_norm is not None:
                        if kw_sub_norm != input_sub_norm:
                            status_category_val.append('Fail')
                            continue

                # If we reached here, Category matches and Subcategory (if required) is consistent
                status_category_val.append('Pass')

            output_df['Category Validation'] = status_category_val
            pass_matrix.append(status_category_val)



            # import re
            # import pandas as pd

            # # ---- Helpers (local scope, NaN-safe) ----
            # def _norm_str(s):
            #     """Normalize for comparison: lower, trim, remove spaces; treat '', 'None', NaN as None."""
            #     if pd.isna(s):
            #         return None
            #     s = str(s).strip()
            #     if s == '' or s.lower() == 'none':
            #         return None
            #     return re.sub(r'\s+', '', s.lower())

            # def _tokenize(s):
            #     """Tokenize text into lowercase alphanumeric tokens; returns [] for None/NaN."""
            #     if pd.isna(s):
            #         return []
            #     s = str(s).lower()
            #     # Split on non-alphanumeric boundaries; underscores/hyphens are treated as separators.
            #     tokens = re.split(r'[^a-z0-9]+', s)
            #     return [t for t in tokens if t]

            # def _normalize_alnum(s):
            #     """Normalize text to a single lowercase alphanumeric string (remove non-alphanumerics)."""
            #     if pd.isna(s):
            #         return ''
            #     return re.sub(r'[^a-z0-9]+', '', str(s).lower())

            # # Minimum length for allowing substring matches to avoid noisy hits like "id" or "db"
            # MIN_SUBSTR_LEN = 3

            # # ---- Load mapping and build keyword index (preprocessed for speed) ----
            # keywords = pd.read_excel("Category_Subcategory_Mapping.xlsx")
            # if not {'Keywords', 'Category'}.issubset(keywords.columns):
            #     raise ValueError("Mapping file must contain 'Keywords' and 'Category' columns")

            # keyword_index = []  # list of tuples: (kw_tokens_fset, kw_cat_norm, kw_sub_norm)
            # has_sub = 'Subcategory' in keywords.columns

            # for _, krow in keywords.iterrows():
            #     kw_tokens = frozenset(_tokenize(krow.get('Keywords')))
            #     if not kw_tokens:
            #         # Skip empty keyword rows
            #         continue
            #     kw_cat_norm = _norm_str(krow.get('Category'))
            #     kw_sub_norm = _norm_str(krow.get('Subcategory')) if has_sub else None
            #     keyword_index.append((kw_tokens, kw_cat_norm, kw_sub_norm))

            # # Prefer the most specific match (largest token set) first
            # keyword_index.sort(key=lambda t: len(t[0]), reverse=True)

            # # ---- Validate each input row ----
            # status_category_val = []
            # has_input_sub = 'Subcategory' in input_df.columns

            # for _, irow in input_df.iterrows():
            #     # 1) Description tokens + normalized alnum string for substring-aware matching
            #     desc_tokens = set(_tokenize(irow.get('Description')))
            #     desc_substr = _normalize_alnum(irow.get('Description'))

            #     if not desc_tokens and not desc_substr:
            #         status_category_val.append('Fail')
            #         continue

            #     # 2) Best match where every keyword token is either a token in the description
            #     #    OR appears as a substring (length >= MIN_SUBSTR_LEN) in the normalized description string.
            #     match = None
            #     for kw_tokens, kw_cat_norm, kw_sub_norm in keyword_index:
            #         # Require all tokens satisfied (token or qualified substring)
            #         all_tokens_match = True
            #         for t in kw_tokens:
            #             if (t in desc_tokens) or (len(t) >= MIN_SUBSTR_LEN and t in desc_substr):
            #                 continue
            #             all_tokens_match = False
            #             break

            #         if all_tokens_match:
            #             match = (kw_cat_norm, kw_sub_norm)
            #             break

            #     if match is None:
            #         status_category_val.append('Fail')
            #         continue

            #     kw_cat_norm, kw_sub_norm = match

            #     # 3) Category comparison (normalized)
            #     input_cat_norm = _norm_str(irow.get('Category'))
            #     if kw_cat_norm is None or input_cat_norm is None or kw_cat_norm != input_cat_norm:
            #         status_category_val.append('Fail')
            #         continue

            #     # 4) Subcategory comparison only if both sides have non-empty values
            #     if has_input_sub:
            #         input_sub_norm = _norm_str(irow.get('Subcategory'))
            #         if kw_sub_norm is not None and input_sub_norm is not None:
            #             if kw_sub_norm != input_sub_norm:
            #                 status_category_val.append('Fail')
            #                 continue

            #     # If we reached here, Category matches and Subcategory (if required) is consistent
            #     status_category_val.append('Pass')

            # # Write results
            # output_df['Category Validation'] = status_category_val
            # pass_matrix.append(status_category_val)

    # Password Check
    if "Password_detected?" in selected_rules:
        required_columns = ["Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Password Check validation")
            print("💡 All entries will FAIL this validation")

        else:
            # UPDATE PASSWORD
            # New Col Added for PASSWORD CHECK
            # Fill NaN values
                input_df['Comments and Work notes'] = input_df['Comments and Work notes'].fillna('')
                input_df['Additional comments'] = input_df['Additional comments'].fillna('')
                password_pattern = re.compile(
                    r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
                )
                # Initialize the status list
                status_password_detected = []

                # Iterate through each row and check for password-like strings
                for idx, row in input_df.iterrows():
                    comment = str(row['Comments and Work notes']).strip()
                    additional = str(row['Additional comments']).strip()

                    # Split into words and check each word against the pattern
                    words = comment.split() + additional.split()
                    detected = any(password_pattern.match(word) for word in words)

                    status_password_detected.append('Fail' if detected else 'Pass')
                    #print(f"Row {idx}: Password detected? {'Fail' if detected else 'Pass'}")

                output_df['password_detected?'] = status_password_detected
                pass_matrix.append(status_password_detected)

    # if "Closed with User Confirmation?" in selected_rules:
    #     required_columns = ["Comments and Work notes", "Additional comments"]
    #     missing_cols = [col for col in required_columns if col not in input_df.columns]
    #     if missing_cols:
    #         print(f"❌ WARNING: Missing columns {missing_cols} for Closed with User Confirmation validation")
    #         print("💡 All entries will FAIL this validation")
            
    #         result = ["Fail - Missing Column"] * len(input_df)
    #         output_df['Closed with User Confirmation?'] = result
    #         pass_matrix.append(["Fail"] * len(input_df))
    #     else:
    #         #     #UPDATED WITH BOTH COLUMNS
    #         #     Closed with User Confirmation?
    #         #     use additional comment
    #         status_user_confirmation = []

    #         for val1, val2 in zip(input_df['Comments and Work notes'], input_df['Additional comments']):
    #             combined_text = ''
                
    #             # Combine both fields if they are not null
    #             if pd.notnull(val1):
    #                 combined_text += str(val1)
    #             if pd.notnull(val2):
    #                 combined_text += ' ' + str(val2)
                
    #             # If both are null or 'None', mark as Pass
    #             if (pd.isnull(val1) and pd.isnull(val2)) or (str(val1).strip().lower() == 'none' and str(val2).strip().lower() == 'none'):
    #                 status_user_confirmation.append('Pass')
    #                 continue

    #             # Normalize and check for confirmation phrases
    #             combined_text = combined_text.replace(' ', '').lower()
    #             if any(phrase in combined_text for phrase in [
    #                 'closingasperuserconfirmation',
    #                 'thankyoufortheconfirmation'
    #                 'clossingwithuserconfirmation',
    #                 'closingwithuserconfirmation',
    #                 'userconfirmedtoclose',
    #                 'closedasperuserconfirmation',
    #                 'usersaidoktocloseticket',
    #                 # Additional combinations
    #                 'confirmedbyusertoclose',
    #                 'userhasconfirmedclosure',
    #                 'useragreedtoclose',
    #                 'userapprovedclosure',
    #                 'userconfirmedticketclosure',
    #                 'userrequestedclosure',
    #                 'useraskedtoclose',
    #                 'userconsenttoclose',
    #                 'usergaveoktoclose',
    #                 'useroktoclose',
    #                 'userconfirmedresolution',
    #                 'userconfirmedissueisresolved',
    #                 'userconfirmeditsfixed',
    #                 'userconfirmednoactionneeded',
    #                 'userconfirmednotsupportrequired',
    #                 'userconfirmedclosureaccepted',
    #                 'userconfirmedfinalstatus',
    #                 'userconfirmedticketcanbeclosed',
    #                 'youcanclosetheticket',
    #                 'youcancloseit',
    #                 'youcanclose',
    #                 'bettertocloseit',
    #                 'bettertoclose',
    #                 'asperuserconfirmationclosing',
    #                 'userconfirmationclosingtheticket',
    #                 'userconfirmationclosing',
    #                 'userconfirmationtoclose',

    #             ]):
    #                 status_user_confirmation.append('Pass')
    #             else:
    #                 status_user_confirmation.append('Fail')
    #         output_df['Closed with User Confirmation?'] = status_user_confirmation
    #         pass_matrix.append(status_user_confirmation)

    
    if "Closed with User Confirmation?" in selected_rules:
        required_columns = ["Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Closed with User Confirmation validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Closed with User Confirmation?'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            status_user_confirmation = []

            # ✅ CHANGE ONLY HERE: Expanded "user confirmation" detection to also catch Slack/Teams/Email/Mail confirmations
            confirmation_keywords = r'''(?ix)
            \b(
                # Explicit confirmation wording
                (user|end\s*user|customer|client|requester)\s+(confirm(?:ed|s|ing)?|acknowledge(?:d|s|ment)?|approve(?:d|s)?|agree(?:d|s)?)
            | confirm(?:ed|s|ing)?
            | confirmation
            | approve(?:d|s)?
            | agreed
            | acknowledged
            | verified
            | validated
            | tested(?:\s+and\s+working)?

                # "Resolved/Fixed" confirmation from user perspective
            | (issue|problem|it|this)\s+(is\s+)?(fixed|resolved|working|work(?:s|ed)|sorted|clear(?:ed)?)
            | (now\s+)?(working\s+fine|works\s+fine|working\s+now|works\s+now)
            | (looks|seems)\s+(good|fine)
            | no\s+issues?\s+(now|anymore)
            | (all\s+)?(good|set|sorted)\b
            | (resolved|fixed)\s+from\s+my\s+side

                # Access/Login success confirmations
            | (able|can)\s+to\s+(login|log\s*in|sign\s*in|access|connect)
            | login\s+(successful|works|working)
            | access\s+(restored|working)
            | (connected|connection)\s+(successful|works|working)

                # Short affirmative replies (common)
            | (ok|okay|kk|k)\b
            | (yes|yep|yup|ya|yeah|y)\b
            | sure\b
            | sure\s+thing
            | sounds\s+good
            | please\s+proceed
            | go\s+ahead
            | proceed

                # Emoji/thumbs-up confirmations (often used in chats)
            | 👍
            | ✅

                # Channel-based confirmations (Slack/Teams/Email/Mail/Outlook etc.)
            | (user|end\s*user|customer|client|requester)\s+replied\b.*\b(confirm|confirmed|acknowledged|all\s+good|working)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b
            | (user|end\s*user|customer|client|requester)\s+confirmed\b.*\b(over|via|on|in|through)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b
            | (user|end\s*user|customer|client|requester)\s+acknowledged\b.*\b(over|via|on|in|through)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b
            | confirmed\b.*\b(over|via|on|in|through)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b
            | acknowledged\b.*\b(over|via|on|in|through)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b
            | reply(?:ied|)\b.*\b(on|in|via|through)\b.*\b(slack|teams|email|e-?mail|mail|outlook)\b.*\b(confirm|confirmed|acknowledged|all\s+good|working)\b
            | email\s+from\s+(user|end\s*user|customer|client|requester)\b.*\b(confirm|confirmed|acknowledged|all\s+good|working)\b
            | mail\s+from\s+(user|end\s*user|customer|client|requester)\b.*\b(confirm|confirmed|acknowledged|all\s+good|working)\b
            )\b
            '''

            # Detect intent to close/resolve/mark resolved (by agent or process)
            closure_keywords = r'''(?ix)
            \b(
                close|closure|closing
            | (proceed|going)\s+to\s+close
            | proceed\b.*to\b.*close
            | good\s+to\s+close
            | we\s+can\s+close
            | you\s+can\s+close
            | please\s+close
            | close\b.*(ticket|incident)
            | (mark|marking)\s+(this|the)?\s*ticket\s+as\s+resolved
            | mark\s*resolved
            | resolve(?:d|)\b
            | resolving\s+the\s+incident
            | (issue|ticket|incident)\s+(is\s+)?(resolved|completed|closed)
            | (upon|post)\s+(your|user)\s+confirmation\b.*(mark|close|resolved)
            | (hence|therefore)\b.*(closing|resolving)\b.*(incident|ticket|issue)
            | thanks\b.*(close|closure)
            )\b
            '''

            for val1, val2 in zip(input_df['Comments and Work notes'], input_df['Additional comments']):
                combined_text = ''
                
                # Combine all comments for the ticket
                if pd.notnull(val1):
                    combined_text += str(val1)
                if pd.notnull(val2):
                    combined_text += ' ' + str(val2)

                combined_text = combined_text.lower()

                # ✅ Check for confirmation + closure keywords anywhere in text
                if re.search(confirmation_keywords, combined_text) and re.search(closure_keywords, combined_text):
                    status_user_confirmation.append('Pass')
                else:
                    status_user_confirmation.append('Fail')

            output_df['Closed with User Confirmation?'] = status_user_confirmation
            pass_matrix.append(status_user_confirmation)



        # Work Notes Updated Regularly
    # if "Work Notes Updated Regularly" in selected_rules:
    #     required_columns = ["Age", "Comments and Work notes", "Additional comments"]
    #     missing_cols = [col for col in required_columns if col not in input_df.columns]
    #     if missing_cols:
    #         print(f"❌ WARNING: Missing columns {missing_cols} for Work Notes Updated Regularly validation")
    #         print("💡 All entries will FAIL this validation")
            
    #         result = ["Fail - Missing Column"] * len(input_df)
    #         output_df['Work Notes Updated Regularly'] = result
    #         pass_matrix.append(["Fail"] * len(input_df))
    #     else:
    #         status_worknotes_updated_regularly = []
    #         threshold_val = thresholds["Work Notes Updated Regularly"]
    #         cnt = 0

    #         for age_val in input_df['Age']:
    #             cnt += 1
    #             if pd.isnull(age_val) or age_val == 'None':
    #                 status_worknotes_updated_regularly.append('Fail')
    #                 continue
    #             if age_val < threshold_val:
    #                 status_worknotes_updated_regularly.append('Pass')
    #             else:
    #                 status_3_Strike_rule_check = three_strike_rule_check()
    #                 strike3 = status_3_Strike_rule_check[cnt - 1]
    #                 status_worknotes_updated_regularly.append(strike3)

    #         output_df['Work Notes Updated Regularly'] = status_worknotes_updated_regularly
    #         pass_matrix.append(status_worknotes_updated_regularly)

    # Ticket Updated Within Business Days
    if "Ticket Updated Within Business Days" in selected_rules:
        required_columns = ["Opened", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for Ticket Updated Within Business Days validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Ticket Updated Within Business Days'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            # add new col 
            # Ticket Updated <48 Hrs (Check worknotes if it is having initial comments withing 48 Hours from created/Opened date)
            # Bussiness days excluded
            import pandas as pd
            import re
            from datetime import datetime
            from pandas.tseries.offsets import BDay
            
            status_ticket_updated = []
            
            for _, row in input_df.iterrows():
                # 1) parse the opened date
                opened_raw = row['Opened']
                try:
                    opened_dt = pd.to_datetime(opened_raw)
                except:
                    status_ticket_updated.append("Invalid Opened Date")
                    continue
                if pd.isnull(opened_dt):
                    status_ticket_updated.append("Invalid Opened Date")
                    continue
                # 2) collect comments
                combined = ''
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notna(row.get(col, '')):
                        combined += '\n' + str(row[col])
                # 3) extract all timestamps using safe parsing
                combined_timestamps = []
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notna(row.get(col, '')):
                        col_timestamps = extract_timestamps_safely(row[col])
                        combined_timestamps.extend(col_timestamps)
                # Filter out any NaT or non-datetime values
                combined_timestamps = [ts for ts in combined_timestamps if not pd.isnull(ts) and hasattr(ts, 'date')]
                if combined_timestamps:
                    first_comment = min(combined_timestamps)
                    if pd.isnull(first_comment):
                        status_ticket_updated.append("Invalid First Comment Date")
                        continue
                    # Convert both to just dates for business day comparison
                    opened_date = opened_dt.date()
                    first_comment_date = first_comment.date()
                    # Calculate business day difference
                    bdays_between = len(pd.bdate_range(opened_date, first_comment_date)) - 1
                    # Accept if first comment is within the configured business days threshold
                    max_days = thresholds.get("ticket_update_days", 2)  # Default to 2 if not specified
                    status_ticket_updated.append('Pass' if bdays_between <= max_days else 'Fail')
                else:
                    status_ticket_updated.append('No Comments')
            output_df['Ticket Updated Within Business Days'] = status_ticket_updated
            pass_matrix.append(status_ticket_updated)
        
    # PA violation Check
    if "Process Adherence Violation Check" in selected_rules:
        required_columns = ["Opened", "Closed", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for PA violation Check validation")
            print("💡 All entries will FAIL this validation")
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['PA violation Check'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            import pandas as pd
            import numpy as np
            from datetime import datetime
            # PA violation Check (Check if ticket was updated every alternate business day from Opened to Closed)
            status_pa_check = []

            for _, row in input_df.iterrows():
                # Parse Opened and Closed dates
                try:
                    opened_dt = pd.to_datetime(row['Opened'])
                    closed_dt = pd.to_datetime(row['Closed'])
                except Exception:
                    status_pa_check.append("Invalid Opened/Closed Date")
                    continue

                # Extract all timestamps from comments
                timestamps = []
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notnull(row.get(col)):
                        timestamps.extend(extract_timestamps_safely(row[col]))

                # Add Opened and Closed as boundary timestamps
                timestamps.append(opened_dt)
                timestamps.append(closed_dt)

                # Filter out any NaT or non-datetime values
                timestamps = [ts for ts in timestamps if not pd.isnull(ts) and hasattr(ts, 'date')]

                # Deduplicate by date and sort
                unique_dates = list(set(ts.date() for ts in timestamps))
                unique_dates = [d for d in unique_dates if d is not None]
                unique_dates.sort()

                # Only proceed if at least two valid dates
                if len(unique_dates) < 2:
                    status_pa_check.append("Invalid Opened/Closed Date")
                    continue

                # Convert to numpy datetime64 for business day calculations
                np_dates = [np.datetime64(date) for date in unique_dates]

                # Calculate business day gaps between consecutive dates
                gaps = []
                for i in range(len(np_dates) - 1):
                    gap = np.busday_count(np_dates[i], np_dates[i + 1])
                    if gap > 0:
                        gaps.append(gap)

                # If any gap > 1, fail
                if any(gap > 1 for gap in gaps):
                    status_pa_check.append("Fail (Gap > 1 business day)")
                else:
                    status_pa_check.append("Pass")

            output_df['PA violation Check'] = status_pa_check
            pass_matrix.append(status_pa_check)

 
    # Work Note Format & Content Check
    if "Work Note Format & Content Check" in selected_rules:
        required_column = "Comments and Work notes"
        if required_column not in input_df.columns:
            print(f"❌ WARNING: Missing column '{required_column}' for Work Note Format & Content Check validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['Work Note Format & Content Check'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            import re
            import pandas as pd

            status_wn_specified = []
            wn_series = input_df['Comments and Work notes']

            # Sentence-ending punctuation marks to check
            SENTENCE_PUNCT = {'.', '!', '?', ';'}

            DATE_REGEX = re.compile(
                    r'\b('
                    r'\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4}'                              # dd/mm/yyyy, mm-dd-yyyy, dd.mm.yyyy
                    r'|'
                    r'\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}'                              # yyyy/mm/dd, yyyy-mm-dd
                    r'|'
                    r'\d{1,2}[-/\. ]?[A-Za-z]{3,9}[-/\. ]?\d{4}'                      # dd-MMM-yyyy or dd Month yyyy
                    r'|'
                    r'[A-Za-z]{3,9}[-/\. ]?\d{1,2},?[-/\. ]?\d{4}'                    # MMM dd, yyyy or Month dd yyyy
                    r'|'
                    r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?'              # ISO datetime
                    r'|'
                    r'\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4}[ ,T]?\s*\d{1,2}:\d{2}(:\d{2})?(\.\d+)?'  # dd/mm/yyyy HH:MM[:SS][.fff]
                    r'|'
                    r'\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}[ ,T]?\s*\d{1,2}:\d{2}(:\d{2})?(\.\d+)?'  # yyyy-mm-dd HH:MM[:SS][.fff]
                    r'|'
                    r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?'      # ISO with timezone
                    r')\b'
                )

            def evaluate_entry(entry: str) -> str:
                """
                Returns one of:
                - "Comprehensive"
                - "Needs improvement"
                - "No match or invalid format"
                """
                try:
                    if entry is None:
                        return "No match or invalid format"

                    entry_str = str(entry).strip()
                    if not entry_str:
                        return "No match or invalid format"

                    # Find the FIRST date occurrence (to match existing behavior)
                    m = DATE_REGEX.search(entry_str)
                    if not m:
                        return "No match or invalid format"

                    date_str = m.group(0)
                    date_index = m.start()

                    # Everything AFTER the date is considered the main content
                    after_date_text = entry_str[m.end():].strip()

                    # Basic content sufficiency checks
                    if len(after_date_text) <= 30:
                        return "Needs improvement"

                    # Count words (split on whitespace)
                    word_count = len(after_date_text.split())
                    if word_count < 10:
                        return "Needs improvement"

                    # Require at least one sentence-ending punctuation mark (., !, ?, ;)
                    if not any(p in after_date_text for p in SENTENCE_PUNCT):
                        return "Needs improvement"

                    # Capitalization check:
                    # Find the first alphabetic character after the date and ensure it's uppercase
                    first_alpha = next((ch for ch in after_date_text if ch.isalpha()), None)
                    if first_alpha is None or not first_alpha.isupper():
                        return "Needs improvement"

                    return "Comprehensive"

                except Exception:
                    return "No match or invalid format"

            for worknote in wn_series:
                # Preserve your legacy handling for nulls and the literal 'None'
                if pd.isnull(worknote) or worknote == 'None':
                    status_wn_specified.append('Fail')
                    continue

                result = evaluate_entry(worknote)
                status_wn_specified.append(result)

            output_df['Work Note Format & Content Check'] = status_wn_specified
            pass_matrix.append(status_wn_specified)

    if "Acknowledgment notes recorded in Worklog?" in selected_rules:
            required_columns = ["Comments and Work notes", "Additional comments"]
            missing_cols = [col for col in required_columns if col not in input_df.columns]

            if missing_cols:
                    print(f"❌ WARNING: Missing columns {missing_cols} for Acknowledgment notes recorded in Worklog? validation")
                    print("💡 All entries will FAIL this validation")
                    result = ["Fail - Missing Column"] * len(input_df)
                    output_df['Acknowledgment notes recorded in Worklog?'] = result
                    pass_matrix.append(["Fail"] * len(input_df))
            else:
                
                import re
                import pandas as pd

                # Hardcoded template from thresholds or UI
                acknowledgment_template = thresholds.get("Acknowledgment_notes_template", "Enter Text here")
                # acknowledgment_template = """Dear User,
                #                             Thank you for contacting us.
                #                             We have received your Service request for the issue you have highlighted.
                #                             Please be assured that we are working on your request and shall provide a next update very soon.
                #                             Regards,
                #                             Support Team"""
                # Normalize template
                norm_template = re.sub(r'\s+', ' ', acknowledgment_template).strip().lower()

                # ✅ Improved regex for timestamps, names, and markers
                timestamp_pattern = re.compile(
                    r'(?:OR)?\s*\d{1,4}[-/]\d{1,2}[-/]\d{1,4}\s+\d{1,2}:\d{2}:\d{2}\s*-\s*[^\n()]+(?:\([^)]+\))?',
                    re.MULTILINE
                )

                def clean_text(text: str) -> str:
                    """Remove timestamps, names, and markers; normalize spaces and lowercase."""
                    if pd.isnull(text):
                        return ''
                    text = str(text)
                    # Remove timestamp + name + markers
                    text = timestamp_pattern.sub('', text)
                    # Normalize spaces and lowercase
                    return re.sub(r'\s+', ' ', text).strip().lower()

                status_acknowledgment = []
                for _, row in input_df.iterrows():
                    combined_text = ''
                    for col in ['Comments and Work notes', 'Additional comments']:
                        combined_text += ' ' + clean_text(row.get(col, ''))
                    combined_text = combined_text.strip()
                    # ✅ Substring match for normalized template
                    if norm_template in combined_text:
                        status_acknowledgment.append('Pass')
                    else:
                        status_acknowledgment.append('Fail')

                output_df['Acknowledgment notes recorded in Worklog?'] = status_acknowledgment
                pass_matrix.append(status_acknowledgment)

    if "Is the resolution summary and closure notes updated as per appropriate template?" in selected_rules:
            required_columns = ["Comments and Work notes", "Additional comments"]
            missing_cols = [col for col in required_columns if col not in input_df.columns]
            if missing_cols:
                print(f"❌ WARNING: Missing columns {missing_cols} for Is the resolution summary and closure notes updated as per appropriate template? validation")
                print("💡 All entries will FAIL this validation")
                result = ["Fail - Missing Column"] * len(input_df)
                output_df['Is the resolution summary and closure notes updated as per appropriate template?'] = result
                pass_matrix.append(["Fail"] * len(input_df))

            else:
                import re
                import pandas as pd
                # Hardcoded template from thresholds or UI
                acknowledgment_template = thresholds.get("Resolution_summary_template", "Enter Text here")
                
                # Normalize template
                norm_template = re.sub(r'\s+', ' ', acknowledgment_template).strip().lower()

                # ✅ Improved regex for timestamps, names, and markers
                timestamp_pattern = re.compile(
                    r'(?:OR)?\s*\d{1,4}[-/]\d{1,2}[-/]\d{1,4}\s+\d{1,2}:\d{2}:\d{2}\s*-\s*[^\n()]+(?:\([^)]+\))?',
                    re.MULTILINE
                )

                def clean_text(text: str) -> str:
                    """Remove timestamps, names, and markers; normalize spaces and lowercase."""
                    if pd.isnull(text):
                        return ''
                    text = str(text)
                    # Remove timestamp + name + markers
                    text = timestamp_pattern.sub('', text)
                    # Normalize spaces and lowercase
                    return re.sub(r'\s+', ' ', text).strip().lower()

                status_acknowledgment = []
                for _, row in input_df.iterrows():
                    combined_text = ''
                    for col in ['Comments and Work notes', 'Additional comments']:
                        combined_text += ' ' + clean_text(row.get(col, ''))
                    combined_text = combined_text.strip()
                    # ✅ Substring match for normalized template
                    if norm_template in combined_text:
                        status_acknowledgment.append('Pass')
                    else:
                        status_acknowledgment.append('Fail')

                output_df['Is the resolution summary and closure notes updated as per appropriate template?'] = status_acknowledgment
                pass_matrix.append(status_acknowledgment)

    #"""Adding a logic 1-1-1 strick check for example if ticket Age is greater than 5 days from created or opened date(As we have Age column also) then check for [additional comments or comments and worknote to get time stamp as above ] is updated or not? as per logic it should be updated for 1-1-1 means every alternate day and 4th on immediate next day of 3rd Update also consider bussiness days only exclude weekends and holidays if 1-1-1 is followed then pass else fail"""
    # Example >> if age of grater than 5 days >> 9july  >> 10july  >> 11july

    # 1-1-1
    if "3 Strike Check(1-1-1)" in selected_rules:
        required_columns = ["Age", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for 3 Strike Check(1-1-1) validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['3 Strike Check(1-1-1)'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:    
            import pandas as pd
            import re
            from datetime import datetime
            from pandas.tseries.offsets import BDay
            import numpy as np

            # Initialize result list
            status_1_1_1_check = []
            input_1_1_1 = thresholds['1-1-1 Check']

            # Iterate over each row in the input DataFrame
            for _, row in input_df.iterrows():
                # Check if ticket age is greater than threshold
                if row.get('Age', 0) <= input_1_1_1:
                    status_1_1_1_check.append("Age <= 3 Days")
                    continue

                # Extract timestamps from both comment columns and combine
                timestamps = []
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notnull(row.get(col)):
                        timestamps.extend(extract_timestamps_safely(row[col]))

                # Deduplicate by date and sort
                unique_dates = list(set(ts.date() for ts in timestamps))
                unique_dates.sort()
                
                if len(unique_dates) < 4:
                    status_1_1_1_check.append("Not Enough Unique Days")
                    continue

                # Convert dates to numpy datetime64 for business day calculations
                np_dates = [np.datetime64(date) for date in unique_dates]
                
                # Calculate business day gaps between consecutive dates
                gaps = []
                for i in range(len(np_dates) - 1):
                    gap = np.busday_count(np_dates[i], np_dates[i + 1])
                    if gap > 0:  # Only include non-zero gaps
                        gaps.append(gap)
                
                # Need at least 3 valid gaps for the pattern
                if len(gaps) < 3:
                    status_1_1_1_check.append("Not Enough Valid Gaps")
                   
                    continue

                # Check the 1-1-1 pattern using the first 3 valid gaps
                first_gap, second_gap, third_gap = gaps[:3]
                
                # 1-1-1 pattern: all gaps must be exactly 1 business day
                if first_gap == 1 and second_gap == 1 and third_gap == 1:
                    status_1_1_1_check.append("Pass")

                else:
                    gap_issues = []
                    if first_gap != 1:
                        gap_issues.append("first")
                    if second_gap != 1:
                        gap_issues.append("second")
                    if third_gap != 1:
                        gap_issues.append("third")
                    msg = f"Fail ({', '.join(gap_issues)} gap(s) ≠ 1 day)"
                    status_1_1_1_check.append(msg)

            # Add result to output DataFrame
            output_df['3 Strike Check(1-1-1)'] = status_1_1_1_check
            pass_matrix.append(status_1_1_1_check)

    # 2-2-1
    if "3 Strike Check(2-2-1)" in selected_rules:
        required_columns = ["Age", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for 3 Strike Check(2-2-1) validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['3 Strike Check(2-2-1)'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        
        else:
            import pandas as pd
            import re
            from datetime import datetime
            from pandas.tseries.offsets import BDay
            import numpy as np

            # Initialize result list
            status_2_2_1_check = []
            input_2_2_1 = thresholds['2-2-1 Check']

            # Iterate over each row in the input DataFrame
            for _, row in input_df.iterrows():
                # Check if ticket age is greater than threshold
                if row.get('Age', 0) <= input_2_2_1:
                    status_2_2_1_check.append("Age <= 3 Days")
                    continue

                # Extract timestamps from both comment columns and combine
                timestamps = []
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notnull(row.get(col)):
                        timestamps.extend(extract_timestamps_safely(row[col]))

                # Deduplicate by date and sort
                unique_dates = list(set(ts.date() for ts in timestamps))
                unique_dates.sort()
                
                if len(unique_dates) < 4:
                    status_2_2_1_check.append("Not Enough Unique Days")
                    continue

                # Convert dates to numpy datetime64 for business day calculations
                np_dates = [np.datetime64(date) for date in unique_dates]
                
                # Calculate business day gaps between consecutive dates
                gaps = []
                for i in range(len(np_dates) - 1):
                    gap = np.busday_count(np_dates[i], np_dates[i + 1])
                    if gap > 0:  # Only include non-zero gaps
                        gaps.append(gap)
                
                # Need at least 3 valid gaps for the pattern
                if len(gaps) < 3:
                    status_2_2_1_check.append("Not Enough Valid Gaps")
                    continue

                # Check the 2-2-1 pattern using the first 3 valid gaps
                first_gap, second_gap, third_gap = gaps[:3]
                
                # 2-2-1 pattern: first two gaps ≤ 2 days, last gap ≤ 1 day
                if first_gap <= 2 and second_gap <= 2 and third_gap <= 1:
                    status_2_2_1_check.append("Pass")
                else:
                    if third_gap > 1:
                        msg = "Fail (Last gap > 1 day)"
                    elif second_gap > 2:
                        msg = "Fail (Second gap > 2 days)"
                    elif first_gap > 2:
                        msg = "Fail (First gap > 2 days)"
                    else:
                        msg = "Fail (Pattern not matched)"
                    status_2_2_1_check.append(msg)

            # Add result to output DataFrame
            output_df['3 Strike Check(2-2-1)'] = status_2_2_1_check
            pass_matrix.append(status_2_2_1_check)

    # 3-2-1
    if "3 Strike Check(3-2-1)" in selected_rules:
        required_columns = ["Age", "Comments and Work notes", "Additional comments"]
        missing_cols = [col for col in required_columns if col not in input_df.columns]
        if missing_cols:
            print(f"❌ WARNING: Missing columns {missing_cols} for 3 Strike Check(3-2-1) validation")
            print("💡 All entries will FAIL this validation")
            
            result = ["Fail - Missing Column"] * len(input_df)
            output_df['3 Strike Check(3-2-1)'] = result
            pass_matrix.append(["Fail"] * len(input_df))
        else:
            import pandas as pd
            import re
            from datetime import datetime
            from pandas.tseries.offsets import BDay
            import numpy as np

            # Initialize result list
            status_3_2_1_check = []
            input_3_2_1 = thresholds['3-2-1 Check']

            # Iterate over each row in the input DataFrame
            for _, row in input_df.iterrows():
                # Check if ticket age is greater than threshold
                if row.get('Age', 0) <= input_3_2_1:
                    status_3_2_1_check.append("Age <= 3 Days")
                    continue

                # Extract timestamps from both comment columns and combine
                timestamps = []
                for col in ['Comments and Work notes', 'Additional comments']:
                    if pd.notnull(row.get(col)):
                        timestamps.extend(extract_timestamps_safely(row[col]))

                # Deduplicate by date and sort
                unique_dates = list(set(ts.date() for ts in timestamps))
                unique_dates.sort()
                
                if len(unique_dates) < 4:
                    status_3_2_1_check.append("Not Enough Unique Days")
                    continue

                # Convert dates to numpy datetime64 for business day calculations
                np_dates = [np.datetime64(date) for date in unique_dates]
                
                # Calculate business day gaps between consecutive dates
                gaps = []
                for i in range(len(np_dates) - 1):
                    gap = np.busday_count(np_dates[i], np_dates[i + 1])
                    if gap > 0:  # Only include non-zero gaps
                        gaps.append(gap)
                
                # Need at least 3 valid gaps for the pattern
                if len(gaps) < 3:
                    status_3_2_1_check.append("Not Enough Valid Gaps")
                    continue

                # Check the 3-2-1 pattern using the first 3 valid gaps
                first_gap, second_gap, third_gap = gaps[:3]
                
                # 3-2-1 pattern: first gap ≤ 3 days, second gap ≤ 2 days, last gap ≤ 1 day
                if first_gap <= 3 and second_gap <= 2 and third_gap <= 1:
                    status_3_2_1_check.append("Pass")
                else:
                    if third_gap > 1:
                        msg = "Fail (Last gap > 1 day)"
                    elif second_gap > 2:
                        msg = "Fail (Second gap > 2 days)"
                    elif first_gap > 3:
                        msg = "Fail (First gap > 3 days)"
                    else:
                        msg = "Fail (Pattern not matched)"
                    status_3_2_1_check.append(msg)

            # Add result to output DataFrame
            output_df['3 Strike Check(3-2-1)'] = status_3_2_1_check
            pass_matrix.append(status_3_2_1_check)


    # Observation1
    #  Comments and Work notes observation
# #    if "Observations1" in selected_rules:
#     punctuation_marks = [".", ",", ";", ":", "!", "?"]
#     timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"  # Matches e.g. 2025-08-27 02:02:24

#     def obs1_feedback(row):
#         entry_str = str(row.get('Comments and Work notes', '')) if not pd.isnull(row.get('Comments and Work notes', '')) else ''
#         addl_val = str(row.get('Additional comments', '')) if 'Additional comments' in row and not pd.isnull(row.get('Additional comments', '')) else ''
#         feedback = set()
#         # Split into remarks by newlines and/or timestamps
#         remarks = re.split(r"\n+|(?=" + timestamp_pattern + ")", entry_str)
#         remarks = [r.strip() for r in remarks if r.strip()]
#         seen_remarks = set()
#         duplicate_found = False
#         for remark in remarks:
#             if remark in seen_remarks:
#                 duplicate_found = True
#             else:
#                 seen_remarks.add(remark)
#             # Word count check
#             word_count = len(remark.split())
#             if word_count < 20:
#                 feedback.add("Too few words")
#             if not any(p in remark for p in punctuation_marks):
#                 feedback.add("Missing punctuation")
#             if len(remark) < 100:
#                 feedback.add("Too short")
#             first_char = remark[0] if remark else ''
#             if not first_char.isupper():
#                 feedback.add("Does not start with capital")
#             # Check for attachment, attachments, or attached (whole word)
#             if re.search(r"\b(attachment[s]?|attached)\b", remark, re.IGNORECASE):
#                 feedback.add("Attachment mentioned")
#         # Also check in Additional comments
#         if re.search(r"\b(attachment[s]?|attached)\b", addl_val, re.IGNORECASE):
#             feedback.add("Attachment mentioned")
#         if duplicate_found:
#             feedback.add("Duplicate comment found")
#         return ", ".join(sorted(feedback))

#     status_wn_observation = input_df.apply(obs1_feedback, axis=1)
#     output_df['Observations1'] = status_wn_observation
#     pass_matrix.append(status_wn_observation.tolist())

# -------------------------
# Observations1 ()
# -------------------------
    import re
    import pandas as pd

    punctuation_marks = [".", ",", ";", ":", "!", "?"]
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"  # Matches e.g., 2025-08-27 02:02:24

    def obs1_feedback(row):
        # Safely extract values
        entry_str = str(row.get('Comments and Work notes', '') or '')
        addl_val = str(row.get('Additional comments', '') or '')

        feedback = set()

        # Split remarks by newlines or timestamps
        remarks = re.split(r"\n+|(?=" + timestamp_pattern + ")", entry_str)
        remarks = [r.strip() for r in remarks if r.strip()]

        seen_remarks = set()
        duplicate_found = False

        for remark in remarks:
            if remark in seen_remarks:
                duplicate_found = True
            else:
                seen_remarks.add(remark)

            # Word count check
            if len(remark.split()) < 20:
                feedback.add("Too few words less")

            # Punctuation check
            if not any(p in remark for p in punctuation_marks):
                feedback.add("Missing punctuation")

            # Length check
            if len(remark) < 100:
                feedback.add("Too short")

            # Capitalization check
            if remark and not remark[0].isupper():
                feedback.add("Does not start with capital")

            # Attachment mention check
            if re.search(r"\b(attachment[s]?|attached)\b", remark, re.IGNORECASE):
                feedback.add("Attachment mentioned")

        # Check Additional comments for attachment mention
        if re.search(r"\b(attachment[s]?|attached)\b", addl_val, re.IGNORECASE):
            feedback.add("Attachment mentioned")

        if duplicate_found:
            feedback.add("Duplicate comment found")

        return ", ".join(sorted(feedback)) if feedback else "Work notes meet quality standards"

    # Apply feedback function row-wise
    status_wn_observation = input_df.apply(obs1_feedback, axis=1)

    # Add to output DataFrame
    output_df['Observations1'] = status_wn_observation

    # Append to pass_matrix for scoring
    pass_matrix.append(status_wn_observation.tolist())

    # Obsevation2
#    if "Obsevations2" in selected_rules:
    # status_feedback_map_observation2 = []

    # def generate_feedback(evaluation):
    #     feedback_map = {
    #         'D': "Short Description has less than thresold char ,",
    #         'E': "Long Description is less than thresold Char ,",
    #         'F': "Response time is more than thresold Min ,",
    #         'G': "Response SLA is breached ,",
    #         'H': "Resolution SLA is breached ,",
    #         'I': "KBA is not tagged to the ticket ,",
    #         'J': "Ticket is reopened ,",
    #         'K': "worknotes is not comprehensive ,",
    #         'L': "Resolution notes is not comprehensive ,",
    #         'M': "Ticket is assigned to incorrect group ,",
    #         'N': "Related record is not tagged ,",
    #         'O': "Ticket is ageing>20 days ,",
    #         'P': "3 Strike rule(Confirmation) is not followed ,",
    #         'Q': "Reassignment count is >3 ,",
    #         'R': "1-1-1 Check is not followed ,",
    #         'S': "2-2-1 Check is not followed ,",
    #         'T': "3-2-1 Check is not followed ,"
    #     }

    #     feedback_parts = [message for key, message in feedback_map.items() if evaluation.get(key) == "Fail"]
    #     return ''.join(feedback_parts) if feedback_parts else "All key quality checks passed for this ticket."
    # # Iterate through the DataFrame
    # for idx, row in output_df.iterrows():
    #     evaluation_dict = {}
    #     if sort_len_obs in row:
    #         evaluation_dict = {"D": row[sort_len_obs],}
    #     if long_desc_obs in row:
    #         evaluation_dict.update({'E' : row[long_desc_obs]})
    #     if response_time_obs in row:
    #         evaluation_dict.update({'F': row[response_time_obs]})
    #     if 'Response SLA Met' in row:
    #         evaluation_dict.update({'G' : row['Response SLA Met']})
    #     if 'Resolution SLA Met' in row:
    #         evaluation_dict= {'H' : row['Resolution SLA Met']}
    #     if 'KBA Tagged?' in row:
    #         evaluation_dict = {'I' : row['KBA Tagged?']}
    #     if 'Reopened?' in row:
    #         evaluation_dict = {'J' : row['Reopened?']}
    #     if worknote_obs in row:
    #         evaluation_dict = {'K' : row[worknote_obs]}
    #     if resolution_notes_obs in row:
    #         evaluation_dict = {'L' : row[resolution_notes_obs]}
    #     if 'assignment group check' in row:
    #         evaluation_dict = {'M': row['Assignment group check']}
    #     if 'Related records tagged?' in row:
    #         evaluation_dict = {'N': row['Related records tagged?']}
    #     if 'ticket ageing check' in row:
    #         evaluation_dict = {'O': row['Ticket Ageing Check']}
    #     if '3 Strike rule remainders check' in row:
    #         evaluation_dict = {'P': row['3 Strike rule remainders check']}
    #     if 'reassignment check?' in row:
    #         evaluation_dict = {'Q': row['Reassignment check?']}
    #     if '1-1-1 Check' in row:
    #         evaluation_dict = {'R': row['1-1-1 Check']}
    #     if '2-2-1 Check' in row:
    #         evaluation_dict = {'S': row['2-2-1 Check']}
    #     if '3-2-1 Check' in row:
    #         evaluation_dict = {'T': row['3-2-1 Check']}

    #     feedback = generate_feedback(evaluation_dict)
    #     status_feedback_map_observation2.append(feedback)

    # output_df['Observations2'] = status_feedback_map_observation2
    # pass_matrix.append(status_feedback_map_observation2)



    # -------------------------
    # Observations2 (DROP-IN)
    # -------------------------
    status_feedback_map_observation2 = []

    def _is_fail(val) -> bool:
        """Robust 'Fail' detector: handles 'Fail', 'Fail - Missing Column', case-insensitive."""
        if val is None:
            return False
        s = str(val).strip().lower()
        return s.startswith("fail")

    def generate_feedback(evaluation: dict) -> str:
        """Build a readable, comma-separated feedback string for failed checks only."""
        feedback_map = {
            'D': "Short Description has less than threshold characters",
            'E': "Long Description is less than threshold characters",
            'F': "Response time is more than threshold minutes",
            'G': "Response SLA is breached",
            'H': "Resolution SLA is breached",
            'I': "KBA is not tagged to the ticket",
            'J': "Ticket is reopened",
            'K': "Worknotes are not comprehensive",
            'L': "Resolution notes are not comprehensive",
            'M': "Ticket is assigned to an incorrect group",
            'N': "Related record is not tagged",
            'O': "Ticket is ageing > 20 days",
            'P': "3 Strike rule (Confirmation) is not followed",
            'Q': "Reassignment count is > 3",
            'R': "1-1-1 Check is not followed",
            'S': "2-2-1 Check is not followed",
            'T': "3-2-1 Check is not followed"
        }
        failed_msgs = [msg for code, msg in feedback_map.items() if _is_fail(evaluation.get(code))]
        return ", ".join(failed_msgs) if failed_msgs else "All key quality checks passed for this ticket."

    def _first_available(row, candidates):
        """Return the first column name from candidates that exists in the row, else None."""
        for name in candidates:
            if name in row.index:
                return name
        return None

    # Build Observations2 row-wise
    for _, row in output_df.iterrows():
        evaluation_dict = {}

        # Dynamic columns produced earlier in your pipeline
        # (keep these variables as you already set them where those checks are computed)
        if 'sort_len_obs' in locals():
            if sort_len_obs in row.index:
                evaluation_dict.update({'D': row[sort_len_obs]})
        if 'long_desc_obs' in locals():
            if long_desc_obs in row.index:
                evaluation_dict.update({'E': row[long_desc_obs]})
        if 'response_time_obs' in locals():
            if response_time_obs in row.index:
                evaluation_dict.update({'F': row[response_time_obs]})
        if 'worknote_obs' in locals():
            if worknote_obs in row.index:
                evaluation_dict.update({'K': row[worknote_obs]})
        if 'resolution_notes_obs' in locals():
            if resolution_notes_obs in row.index:
                evaluation_dict.update({'L': row[resolution_notes_obs]})

        # Fixed-name columns (with tolerant variants)
        col = _first_available(row, ['Response SLA Met', 'Response SLA met', 'Response SLA'])
        if col: evaluation_dict.update({'G': row[col]})

        col = _first_available(row, ['Resolution SLA Met', 'Resolution SLA met', 'Resolution SLA'])
        if col: evaluation_dict.update({'H': row[col]})

        col = _first_available(row, ['KBA Tagged?', 'KBA Tagged'])
        if col: evaluation_dict.update({'I': row[col]})

        col = _first_available(row, ['Reopened?', 'Reopened'])
        if col: evaluation_dict.update({'J': row[col]})

        col = _first_available(row, ['Assignment group check', 'Assignment Group Check'])
        if col: evaluation_dict.update({'M': row[col]})

        col = _first_available(row, ['Related records tagged?', 'Related Records Tagged?'])
        if col: evaluation_dict.update({'N': row[col]})

        col = _first_available(row, ['Ticket Ageing Check', 'Ticket ageing Check', 'Ticket Ageing check'])
        if col: evaluation_dict.update({'O': row[col]})

        col = _first_available(row, [
            '3 Strike rule remainders check',
            '3 Strike rule check(escalation policy check for Remainder)',
            '3 Strike rule check (escalation policy check for Remainder)'
        ])
        if col: evaluation_dict.update({'P': row[col]})

        col = _first_available(row, ['Reassignment check?', 'Reassignment Check?'])
        if col: evaluation_dict.update({'Q': row[col]})

        # 3-Strike variants (support both your output names)
        col = _first_available(row, ['3 Strike Check(1-1-1)', '1-1-1 Check'])
        if col: evaluation_dict.update({'R': row[col]})

        col = _first_available(row, ['3 Strike Check(2-2-1)', '2-2-1 Check'])
        if col: evaluation_dict.update({'S': row[col]})

        col = _first_available(row, ['3 Strike Check(3-2-1)', '3-2-1 Check'])
        if col: evaluation_dict.update({'T': row[col]})

        # Compose feedback
        feedback = generate_feedback(evaluation_dict)
        status_feedback_map_observation2.append(feedback)

    # Attach to output
    output_df['Observations2'] = status_feedback_map_observation2

    # (If pass_matrix is expected to include this narrative list, keep as is)
    pass_matrix.append(status_feedback_map_observation2)


    # Scoring
    # for i in range(len(input_df)):
    #     cnt_pass = sum(1 for check in pass_matrix if check[i] == "Pass")
    #     percent = (cnt_pass / total_checks) * 100 if total_checks else 0
    #     percent = math.ceil(percent)
    #     score_list.append(percent)
    #     print(total_checks)
    
    #Only change inside your loop
    for i in range(len(input_df)):
        # Count passes as before
        cnt_pass = sum(1 for check in pass_matrix if check[i] == "Pass")
        # NEW: denominator = only Pass + Fail (ignores "No data found", "Not Enough Comments", blanks, etc.)
        denom = sum(1 for check in pass_matrix if check[i] in ("Pass", "Fail"))
        percent = math.ceil((cnt_pass * 100) / denom) if denom else 0
        score_list.append(percent)

        print(total_checks)  # left untouched to keep your logging the same

        if percent < 75:
            score_category.append('<75%')
        elif percent < 90:
            score_category.append('75%-90%')
        else:
            score_category.append('>90%-100%')
 
    output_df["Score"] = score_list
    output_df["Score Category"] = score_category

    # ----------------------------------------------------------------------
    # NEW: Weightage Score (adds a second score column without touching old Score)
    # ----------------------------------------------------------------------

    def _normalize_rule_name(name: str) -> str:
        """Normalize rule labels so '&amp;' in UI matches '&' in DataFrame columns."""
        return str(name).replace("&amp;", "&").strip()

    # Build a lookup from normalized column name -> actual DataFrame column
    normalized_col_map = {
        _normalize_rule_name(col).lower(): col for col in output_df.columns
    }

    # Mapping from UI rule names to DataFrame columns
    RULE_TO_COLUMN = {
        # validation rule name : Output column name
        "Short Description Length Check": "Short Description",
        "Long Description Length Check": "Long Description",
        "Actual Response Time took": "Response Time",
        "Response SLA Met ?": "Response SLA Met",
        "Resolution SLA Met ?": "Resolution SLA Met",
        "KBA Tagged?": "KBA Tagged?",
        "Reopened ?": "Reopened?",
        "Related records tagged?": "Related records tagged?",
        #"Work Notes Updated Regularly": "Work Notes Updated Regularly",
        "Ticket Updated Within Business Days": "Ticket Updated Within Business Days",
        "Process Adherence Violation Check": "PA violation Check",
        "Work notes Length Check": "Work notes Length",
        "Resolution Notes / Additional comment Length Check": "Additional comments",
        "Assignment group check": "Assignment group check",
        "Related records tagged?": "Related records tagged?",
        "Ticket Ageing Check": "Ticket Ageing Check",
        "Reassignment check?": "Reassignment check?",
        "Priority Validation": "Priority Validation",
        "Category Validation": "Category Validation",
        "Right Pending Justification Usage": "Pending Justification",
        "Password_detected?": "Password Check",
        "Has Attachments": "Has Attachments",
        "Closed with User Confirmation?": "Closed with User Confirmation?",
        #"Work Notes Updated Regularly": "Work Notes Updated Regularly",
        "Ticket Updated Within Business Days": "Ticket Updated Within Business Days",
        "Process Adherence Violation Check": "PA violation Check",
        "3 Strike rule check(escalation policy check for Remainder)": "3 Strike rule remainders check",
        "Work Note Format & Content Check": "Work Note Format & Content Check",
        "3 Strike Check(1-1-1)": "3 Strike Check(1-1-1)",
        "3 Strike Check(2-2-1)": "3 Strike Check(2-2-1)",
        "3 Strike Check(3-2-1)": "3 Strike Check(3-2-1)",
        "Acknowledgment notes recorded in Worklog?": "Acknowledgment notes recorded in Worklog?",
        "Is the resolution summary and closure notes updated as per appropriate template?": "Is the resolution summary and closure notes updated as per appropriate template?"
    }

    # # Debug prints to diagnose weightage score issues
    # print("Normalized column map:", normalized_col_map)
    # print("Weights dict:", weights)
    # print("DataFrame columns:", list(output_df.columns))

    use_weights = isinstance(weights, dict) and len(weights) > 0

    weightage_scores = []
    for i in range(len(input_df)):
        if use_weights:
            total_possible = 0
            total_earned = 0
            for rule_name, w in weights.items():
                # Use the mapping to get the correct DataFrame column
                df_col = RULE_TO_COLUMN.get(rule_name)
                if not df_col or df_col not in output_df.columns:
                    continue
                status = str(output_df.at[i, df_col]).strip().lower()
                print(f"Row {i}, Rule: {rule_name}, Col: {df_col}, Status: {status}, Weight: {w}")
                if status in ("pass", "fail") and w > 0:
                    total_possible += w
                    if status == "pass":
                        total_earned += w
            if total_possible > 0:
                variable = int(math.ceil((total_earned * 100) / total_possible))
                weightage_scores.append(variable)
                print(f"Weightage Score for row {i}: {variable}")
            else:
                weightage_scores.append(0)
        else:
            weightage_scores.append(score_list[i])  # fallback if weights not used
        
    output_df["Weightage Score"] = weightage_scores


    # --- return as before ---

    return output_df
