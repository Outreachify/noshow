from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import timedelta
from dateutil import parser
from dateutil.parser import parse as parse_dt
import datetime
import pytz
import logging
import re
import time
from functools import wraps
import requests
import urllib.parse
import msal
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Environment Config ===
SERVICE_ACCOUNT_FILE = os.environ.get('SERVICE_ACCOUNT_FILE', 'giga-green-meet-api-7ec88a255f0c.json')
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')  # Alternative: JSON string
DELEGATED_ADMIN_EMAIL = os.environ.get('DELEGATED_ADMIN_EMAIL', 'admin@giga.green')
ORG_DOMAIN = os.environ.get('ORG_DOMAIN', 'giga.green')
SCOPES = ['https://www.googleapis.com/auth/admin.reports.audit.readonly', 'https://www.googleapis.com/auth/calendar.readonly']

# Microsoft Graph API Configuration
MICROSOFT_CLIENT_ID = os.environ.get('MICROSOFT_CLIENT_ID')
MICROSOFT_CLIENT_SECRET = os.environ.get('MICROSOFT_CLIENT_SECRET')
MICROSOFT_TENANT_ID = os.environ.get('MICROSOFT_TENANT_ID')

# Validate required environment variables
required_env_vars = ['MICROSOFT_CLIENT_ID', 'MICROSOFT_CLIENT_SECRET', 'MICROSOFT_TENANT_ID']
missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# MSAL Configuration
AUTHORITY = f"https://login.microsoftonline.com/{MICROSOFT_TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_BETA = "https://graph.microsoft.com/beta"

# === Auth Setup for Google ===
def get_google_credentials():
    """Get Google service account credentials from file or environment variable"""
    if SERVICE_ACCOUNT_JSON:
        # Use JSON string from environment variable
        service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
        return service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        ).with_subject(DELEGATED_ADMIN_EMAIL)
    else:
        # Use JSON file
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        ).with_subject(DELEGATED_ADMIN_EMAIL)

credentials = get_google_credentials()
service = build('admin', 'reports_v1', credentials=credentials)

INTERNAL_EMAILS = {
    "admin@giga.green", "j.bahr@giga.green", "a.hussien@giga.green", "j.starke@giga.green",
    "i.stehle@giga.green", "developers@giga.green", "v.jung@giga.green", "l.zainab@giga.green",
    "c.schneider@giga.green", "l.kebede@giga.green", "m.wilke@giga.green", "n.geiger@giga.green",
    "r.nowak@giga.green", "k.lodhari@giga.green", "m.petalcorin@giga.green", "j.patel@giga.green",
    "f.denner@giga.green", "a.lipp@giga.green", "d.wagener@giga.green", "a.leipold@giga.green",
    "m.michael@giga.green", "n.tomas@giga.green", "s.schmidt@giga.green", "beratung@giga.green",
    "w.roebig@giga.green", "r.liebig@giga.green", "eddie.esche@digital-act.de", "o.misczyk@giga.green",
    "rimbas.itb@gmail.com", "o.tekdal@giga.green", "marketing@giga.green", "finance@giga.green",
    "f.hirschler@giga.green", "testing@giga.green", "a.friedrich@giga.green", "c.sinne@giga.green",
    "m.krettek@giga.green", "p.steuding@giga.green", "s.harvey@giga.green", "e.wuensch@giga.green",
    "k.ulrich@giga.green", "b.wagener-berg@giga.green", "n.weber@giga.green", "partnership@giga.green",
    "matthias@addsales.io", "partner@giga.green", "a.werner@giga.green", "s.geffers@giga.green",
    "b.laub@giga.green", "ti@yoyaba.com", "ts@yoyaba.com", "iy@yoaba.com", "ido2@yoyaba.com",
    "kieran@addsales.io", "v.kirchhoefer@giga.green", "j.lange@giga.green", "b.ajdinovic@giga.green",
    "f.neuhaus@giga.green", "c.thiemann@giga.green", "a.jawad@giga.green"
}

# ===================== GOOGLE MEET FUNCTIONS =====================

def get_calendar_credentials(user_email):
    if SERVICE_ACCOUNT_JSON:
        service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
        return service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        ).with_subject(user_email)
    else:
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        ).with_subject(user_email)

def extract_meet_id(link):
    import re
    if not link:
        return None
    match = re.search(r'meet\.google\.com/([\w-]+)', link)
    if match:
        meet_id = match.group(1).replace('-', '')
        return meet_id
    return None

def find_google_meet_id(start_time_dt, end_time_dt, invitee):
    # Search all internal calendars for the event, return the first Google Meet ID found
    from googleapiclient.discovery import build
    for email in INTERNAL_EMAILS:
        try:
            creds = get_calendar_credentials(email)
            cal_service = build('calendar', 'v3', credentials=creds)
            # Use a 5 minute buffer for time
            time_buffer = timedelta(minutes=60)
            events_result = cal_service.events().list(
                calendarId=email,
                timeMin=(start_time_dt - time_buffer).isoformat(),
                timeMax=(end_time_dt + time_buffer).isoformat(),
                singleEvents=True
            ).execute()
            events = events_result.get('items', [])
            for event in events:
                event_start_str = event.get('start', {}).get('dateTime', '')
                event_end_str = event.get('end', {}).get('dateTime', '')
                if not event_start_str or not event_end_str:
                    continue
                event_start = parser.isoparse(event_start_str)
                event_end = parser.isoparse(event_end_str)
                # Within 60 min tolerance
                if (abs(event_start - start_time_dt) > timedelta(minutes=60) or 
                    abs(event_end - end_time_dt) > timedelta(minutes=60)):
                    continue
                attendees_raw = event.get('attendees', [])
                attendees = [a.get('email', '').lower() for a in attendees_raw if 'email' in a]
                if invitee.lower() not in attendees:
                    continue
                hangout_link = event.get('hangoutLink', '')
                if hangout_link and 'meet.google.com' in hangout_link:
                    meet_id = extract_meet_id(hangout_link)
                    if meet_id:
                        logger.info(f"[GOOGLE MEET] Found Google Meet ID: {meet_id} in {email}'s calendar")
                        return meet_id, hangout_link
        except Exception as e:
            logger.warning(f"Error searching {email}'s calendar: {e}")
            continue
    logger.warning("[GOOGLE MEET] No matching meeting found in any calendar.")
    return None, None

def check_google_meet(start_time_str, invitee):
    """Check Google Meet meetings - uses only start_time internally"""
    try:
        # Parse start time
        start_time_dt = parser.isoparse(start_time_str)
        # Google Meet logic: always use start_time + 1 hour for end_time
        end_time_dt = start_time_dt + timedelta(hours=1)
        
        # Always output in UTC ISO with 'Z'
        start_time = start_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = end_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info(f"[GOOGLE MEET] Searching for meetings between {start_time} and {end_time}")
        logger.info(f"[GOOGLE MEET] Looking for invitee: {invitee}")
        
        # Find Google Meet ID from calendar
        calendar_meeting_id, meet_link = find_google_meet_id(start_time_dt, end_time_dt, invitee)
        
        if not calendar_meeting_id:
            return {
                "platform": "Google Meet",
                "meeting_found": False,
                "meeting_link": None,
                "start_time": start_time,
                "end_time": end_time,
                "no_show": "NA",  # Meeting link not found in calendar
                "external_participants": [],
                "message": "No Google Meet event found in calendar"
            }
        
        # Fetch Google Meet activities
        response = service.activities().list(
            userKey='all',
            applicationName='meet',
            eventName='call_ended',
            startTime=start_time,
            endTime=end_time
        ).execute()
        
        meetings = {}
        for activity in response.get('items', []):
            actor = activity.get("actor", {})
            activity_time = activity.get("id", {}).get("time", "unknown")
            for event in activity.get("events", []):
                params = {p['name']: p.get('value') or p.get('boolValue') for p in event.get("parameters", [])}
                meeting_code = params.get("meeting_code", "").lower()
                if not meeting_code:
                    continue
                if meeting_code not in meetings:
                    meetings[meeting_code] = {
                        "attendees": set(),
                        "internal_participants": set(),
                        "external_participants": set(),
                        "host": params.get("organizer_email"),
                        "time": activity_time
                    }
                email = params.get("identifier") or actor.get("email") or ""
                email = email.lower()
                is_email = "@" in email
                is_internal = email.endswith("@" + ORG_DOMAIN)
                is_external = is_email and not is_internal
                if is_email:
                    meetings[meeting_code]["attendees"].add(email)
                    if is_internal:
                        meetings[meeting_code]["internal_participants"].add(email)
                    else:
                        meetings[meeting_code]["external_participants"].add(email)
        
        # Check if the calendar meeting was found in audit logs
        if calendar_meeting_id in meetings:
            m = meetings[calendar_meeting_id]
            external_joined = sorted(m["external_participants"])
            no_show = len(external_joined) == 0
            
            return {
                "platform": "Google Meet",
                "meeting_found": True,
                "meeting_link": meet_link,
                "start_time": start_time,
                "end_time": end_time,
                "no_show": no_show,
                "external_participants": external_joined,
                "total_attendees": len(m["attendees"]),
                "internal_count": len(m["internal_participants"]),
                "external_count": len(m["external_participants"])
            }
        else:
            return {
                "platform": "Google Meet",
                "meeting_found": False,
                "meeting_link": meet_link,
                "start_time": start_time,
                "end_time": end_time,
                "no_show": "no data",  # Meeting found in calendar but no attendance data
                "external_participants": [],
                "message": "Meeting found in calendar but no attendance data in audit logs"
            }
            
    except ValueError as e:
        logger.error(f"[GOOGLE MEET] Invalid datetime format: {str(e)}")
        return {
            "platform": "Google Meet",
            "meeting_found": False,
            "meeting_link": None,
            "start_time": start_time_str,
            "end_time": None,
            "no_show": "incorrect data",
            "error": f"Invalid datetime format: {str(e)}"
        }
    except Exception as e:
        logger.error(f"[GOOGLE MEET] Error: {str(e)}")
        return {
            "platform": "Google Meet",
            "meeting_found": False,
            "meeting_link": None,
            "start_time": start_time_str,
            "end_time": None,
            "no_show": "NA",
            "error": str(e)
        }

# ===================== MICROSOFT TEAMS FUNCTIONS =====================

def validate_datetime_input(datetime_str):
    try:
        return parser.isoparse(datetime_str)
    except Exception as e:
        raise ValueError(f"Invalid datetime format: {datetime_str}")

def detect_teams_meeting(event):
    """Detect if meeting is Teams and return the link"""
    location = event.get('location', '').lower()
    description = event.get('description', '').lower()
    
    teams_indicators = ['teams.microsoft.com', 'teams.live.com', 'teams meeting', 'calendly.com']
    
    for indicator in teams_indicators:
        if indicator in location or indicator in description:
            if 'calendly.com' in location or 'calendly.com' in description:
                calendly_match = re.search(r'https://[^\s]*calendly[^\s]*', location + ' ' + description)
                if calendly_match:
                    return calendly_match.group(0)
            
            teams_link_match = re.search(r'https://[^\s]*teams[^\s]*', location + ' ' + description)
            if teams_link_match:
                return teams_link_match.group(0)
    
    return None

def extract_teams_id_from_direct_url(teams_url):
    """Extract Teams meeting ID from direct Teams URLs with URL decoding"""
    if not teams_url:
        return None
    
    original_url = teams_url
    if '%' in teams_url:
        teams_url = urllib.parse.unquote(teams_url)
        logger.info(f"URL decoded from: {original_url}")
        logger.info(f"URL decoded to: {teams_url}")
    
    patterns = [
        r'(19:meeting_[^/\s@\?]+@thread\.v2)',
        r'(meeting_[^/\s@\?]+@thread\.v2)',
        r'19%3Ameeting_([^%/\s@\?]+)%40thread\.v2',
        r'meeting_([^%/\s@\?]+)%40thread\.v2',
        r'teams\.microsoft\.com/.*meetup-join/(\d+)/(\w+)',
        r'teams\.microsoft\.com/l/meetup-join/([^/\?]+)',
        r'thread\.v2/([^/\?]+)',
        r'meetingId=([^&\s]+)',
        r'conference[Ii]d=([^&\s]+)',
        r'teams\.live\.com/meet/([^/\?]+)',
        r'orgid=([^&\s]+)',
        r'meetings/([A-Za-z0-9+/=_-]+)',
        r'join/([A-Za-z0-9+/=_-]+)',
    ]
    
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, teams_url, re.IGNORECASE)
        if match:
            if i < 4:
                if match.group(0).startswith('19:'):
                    meeting_id = match.group(0)
                elif match.group(0).startswith('meeting_'):
                    meeting_id = f"19:{match.group(0)}"
                else:
                    encoded_part = match.group(1)
                    meeting_id = f"19:meeting_{encoded_part}@thread.v2"
            else:
                meeting_id = match.group(1) if len(match.groups()) == 1 else f"{match.group(1)}_{match.group(2)}"
            
            logger.info(f"Extracted Teams meeting ID using pattern {i}: {meeting_id}")
            return meeting_id
    
    logger.warning(f"Could not extract meeting ID from Teams URL: {teams_url}")
    return None

def extract_teams_meeting_id_enhanced(teams_link):
    """Enhanced Teams meeting ID extraction with Calendly redirect following"""
    if not teams_link:
        return None
    
    if 'calendly.com' in teams_link:
        try:
            logger.info(f"Following Calendly redirect for: {teams_link}")
            
            session = requests.Session()
            session.max_redirects = 10
            
            response = session.head(teams_link, allow_redirects=True, timeout=20)
            final_url = response.url
            
            logger.info(f"Calendly redirect led to: {final_url}")
            
            if 'teams.microsoft.com' in final_url or 'teams.live.com' in final_url:
                teams_id = extract_teams_id_from_direct_url(final_url)
                if teams_id:
                    logger.info(f"Successfully extracted Teams ID from Calendly redirect: {teams_id}")
                    return teams_id
            
            try:
                logger.info("HEAD request didn't find Teams URL, trying GET request...")
                get_response = session.get(teams_link, allow_redirects=True, timeout=20)
                
                if get_response.status_code == 200:
                    content = get_response.text
                    
                    teams_patterns = [
                        r'https://teams\.microsoft\.com/l/meetup-join/[^\s"\']+',
                        r'https://teams\.live\.com/meet/[^\s"\']+',
                        r'"joinUrl":"([^"]*teams[^"]*)"',
                        r'teams\.microsoft\.com[^"\']*',
                        r'19:meeting_[^"\'@\s]+@thread\.v2',
                        r'meeting_[^"\'@\s]+@thread\.v2'
                    ]
                    
                    for pattern in teams_patterns:
                        match = re.search(pattern, content, re.IGNORECASE)
                        if match:
                            teams_url = match.group(1) if 'joinUrl' in pattern else match.group(0)
                            if not teams_url.startswith('http') and ('meeting_' in teams_url or 'thread.v2' in teams_url):
                                if not teams_url.startswith('19:'):
                                    teams_url = f"19:{teams_url}"
                                logger.info(f"Extracted meeting ID from page content: {teams_url}")
                                return teams_url
                            else:
                                teams_id = extract_teams_id_from_direct_url(teams_url)
                                if teams_id:
                                    logger.info(f"Extracted from page content URL: {teams_id}")
                                    return teams_id
                                    
            except Exception as content_e:
                logger.warning(f"Failed to parse Calendly page content: {content_e}")
            
            match = re.search(r'calendly\.com/events/([^/\?]+)', teams_link)
            if match:
                calendly_id = f"calendly_{match.group(1)}"
                logger.info(f"Using Calendly ID as fallback: {calendly_id}")
                return calendly_id
                
        except Exception as e:
            logger.warning(f"Failed to follow Calendly redirect: {e}")
            match = re.search(r'calendly\.com/events/([^/\?]+)', teams_link)
            if match:
                return f"calendly_{match.group(1)}"
    
    return extract_teams_id_from_direct_url(teams_link)

def get_app_token():
    """Get application token using client credentials"""
    app = msal.ConfidentialClientApplication(
        MICROSOFT_CLIENT_ID, authority=AUTHORITY, client_credential=MICROSOFT_CLIENT_SECRET
    )
    token_resp = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in token_resp:
        raise RuntimeError(f"Token error: {token_resp.get('error_description')}")
    return token_resp["access_token"]

def find_meetings_in_timerange(token, start_time, end_time):
    """Find all meetings in a specific time range"""
    headers = {"Authorization": f"Bearer {token}"}
    
    if start_time.endswith('Z'):
        start_dt = datetime.datetime.fromisoformat(start_time.replace('Z', '+00:00'))
    else:
        start_dt = datetime.datetime.fromisoformat(start_time)
    
    if end_time.endswith('Z'):
        end_dt = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
    else:
        end_dt = datetime.datetime.fromisoformat(end_time)
    
    start_filter = (start_dt - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_filter = (end_dt + timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    from urllib.parse import quote
    filter_query = f"startDateTime ge {start_filter} and startDateTime lt {end_filter}"
    url = f"{GRAPH_BETA}/communications/callRecords?$filter={quote(filter_query)}"
    
    logger.info(f"[TEAMS] Searching for meetings between: {start_time} and {end_time}")
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("value", [])
            logger.info(f"[TEAMS] Found {len(records)} meeting(s) in this time range")
            return records
        else:
            logger.error(f"[TEAMS] Error: {resp.status_code} - {resp.text}")
            return []
    except Exception as e:
        logger.error(f"[TEAMS] Exception: {e}")
        return []

def get_call_record_details(token, record_id):
    """Get full call record with sessions and segments to extract participants"""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BETA}/communications/callRecords/{record_id}?$expand=sessions($expand=segments)"
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    
    return None

def extract_participant_info(endpoint, role, seg_start, seg_end):
    """Extract participant information from caller/callee endpoint"""
    identity = endpoint.get("identity", {})
    
    if not identity:
        return None
    
    participant = {
        'id': 'unknown',
        'displayName': 'Unknown',
        'type': 'unknown',
        'role': role,
        'segments': [{'start': seg_start, 'end': seg_end}]
    }
    
    if "user" in identity:
        user = identity.get("user", {})
        if user:
            participant.update({
                'id': user.get("id", "unknown_user"),
                'displayName': user.get("displayName", "Unknown User"),
                'type': 'user'
            })
    elif "phone" in identity:
        phone = identity.get("phone", {})
        if phone:
            participant.update({
                'id': f"phone_{phone.get('id', 'unknown')}",
                'displayName': phone.get("displayName", "Phone User"),
                'type': 'phone'
            })
    elif "guest" in identity:
        guest = identity.get("guest", {})
        if guest:
            participant.update({
                'id': f"guest_{guest.get('id', 'unknown')}",
                'displayName': guest.get("displayName", "Guest User"),
                'type': 'guest'
            })
    elif "application" in identity:
        app = identity.get("application", {})
        if app:
            participant.update({
                'id': f"app_{app.get('id', 'unknown')}",
                'displayName': app.get("displayName", "Application"),
                'type': 'application'
            })
    
    return participant

def extract_all_participants(call_record):
    """Extract all unique participants from a call record"""
    participants = {}
    
    record_id = call_record.get("id", "unknown")
    start_time = call_record.get("startDateTime", "")
    end_time = call_record.get("endDateTime", "")
    
    sessions = call_record.get("sessions", [])
    
    for session in sessions:
        segments = session.get("segments", [])
        
        for segment in segments:
            seg_start = segment.get("startDateTime", "")
            seg_end = segment.get("endDateTime", "")
            
            caller = segment.get("caller", {})
            if caller:
                participant = extract_participant_info(caller, "caller", seg_start, seg_end)
                if participant:
                    key = participant['id']
                    if key in participants:
                        participants[key]['segments'].append({
                            'start': seg_start,
                            'end': seg_end
                        })
                    else:
                        participants[key] = participant
            
            callee = segment.get("callee", {})
            if callee:
                participant = extract_participant_info(callee, "callee", seg_start, seg_end)
                if participant:
                    key = participant['id']
                    if key in participants:
                        participants[key]['segments'].append({
                            'start': seg_start,
                            'end': seg_end
                        })
                    else:
                        participants[key] = participant
    
    return {
        'record_id': record_id,
        'start_time': start_time,
        'end_time': end_time,
        'participants': participants
    }

def get_user_details(token, user_id):
    """Get user details like email"""
    if not user_id or user_id.startswith(('phone_', 'guest_', 'app_')) or user_id == 'unknown_user':
        return None
    
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    
    return None

def check_organizer_in_meeting(participants, organizer_email, token):
    """Check if organizer email is found in any participant's email"""
    if not organizer_email:
        return True, None
    
    target_email = organizer_email.lower().strip()
    
    for pid, info in participants.items():
        if info['type'] == 'user' and not pid.startswith(('phone_', 'guest_', 'app_', 'unknown')):
            user_details = get_user_details(token, pid)
            if user_details:
                email = user_details.get("mail", "").lower()
                upn = user_details.get("userPrincipalName", "").lower()
                
                if email and target_email == email:
                    return True, user_details.get("displayName", info['displayName'])
                if upn and target_email == upn:
                    return True, user_details.get("displayName", info['displayName'])
    
    return False, None

def check_external_participants(participants, token):
    """Check if any external participants attended the meeting"""
    external_count = 0
    external_participants = []
    
    for pid, info in participants.items():
        display_name = info.get('displayName', '')
        if display_name is None:
            display_name = ''
        display_name = display_name.strip()
        
        if not display_name or display_name.lower() == 'none':
            continue
            
        if info['type'] == 'user' and not pid.startswith(('phone_', 'guest_', 'app_', 'unknown')):
            user_details = get_user_details(token, pid)
            if user_details:
                email = user_details.get("mail") or user_details.get("userPrincipalName", "")
                if email:
                    if email.lower() not in [e.lower() for e in INTERNAL_EMAILS]:
                        external_count += 1
                        external_participants.append({
                            'email': email,
                            'displayName': user_details.get("displayName", display_name),
                            'type': 'external_user'
                        })
                        logger.info(f"[TEAMS] Found external participant with email: {email}")
                else:
                    external_count += 1
                    external_participants.append({
                        'email': None,
                        'displayName': display_name,
                        'type': 'external_no_email'
                    })
                    logger.info(f"[TEAMS] Found external participant without email: {display_name}")
            else:
                external_count += 1
                external_participants.append({
                    'email': None,
                    'displayName': display_name,
                    'type': 'external_no_details'
                })
                logger.info(f"[TEAMS] Found external participant (no details): {display_name}")
        elif info['type'] in ['phone', 'guest', 'unknown']:
            external_count += 1
            external_participants.append({
                'email': None,
                'displayName': display_name,
                'type': info['type']
            })
            logger.info(f"[TEAMS] Found external {info['type']} participant: {display_name}")
    
    logger.info(f"[TEAMS] Total external participants found: {external_count}")
    return external_count > 0, external_participants

def check_teams_meeting(start_time_str, end_time_str, invitee):
    """Check Microsoft Teams meetings - uses both start_time and end_time"""
    try:
        start_time = validate_datetime_input(start_time_str)
        end_time = validate_datetime_input(end_time_str)
        invitee_email = invitee.lower()
        
        logger.info(f"[TEAMS] Searching for Teams meetings between {start_time} and {end_time} with invitee {invitee_email}")
        
        # Step 1: Find Teams meetings in Google Calendar
        google_meetings = []
        seen_meetings = set()
        
        for email in INTERNAL_EMAILS:
            try:
                creds = get_calendar_credentials(email)
                cal_service = build('calendar', 'v3', credentials=creds)
                
                events_result = cal_service.events().list(
                    calendarId=email,
                    timeMin=start_time.isoformat(),
                    timeMax=end_time.isoformat(),
                    singleEvents=True
                ).execute()
                
                events = events_result.get('items', [])
                
                for event in events:
                    event_start_str = event.get('start', {}).get('dateTime', '')
                    event_end_str = event.get('end', {}).get('dateTime', '')
                    if not event_start_str or not event_end_str:
                        continue
                    
                    event_start = parse_dt(event_start_str)
                    event_end = parse_dt(event_end_str)
                    
                    time_tolerance = timedelta(minutes=15)
                    if (abs(event_start - start_time) > time_tolerance or 
                        abs(event_end - end_time) > time_tolerance):
                        continue
                    
                    attendees_raw = event.get('attendees', [])
                    attendees = [a.get('email', '').lower() for a in attendees_raw if 'email' in a]
                    
                    if invitee_email not in attendees:
                        continue
                    
                    teams_link = detect_teams_meeting(event)
                    if not teams_link:
                        continue
                    
                    teams_id = extract_teams_meeting_id_enhanced(teams_link)
                    
                    meeting_key = f"{teams_link}_{teams_id}" if teams_id else teams_link
                    
                    if meeting_key in seen_meetings:
                        continue
                    
                    seen_meetings.add(meeting_key)
                    
                    google_meetings.append({
                        "start_time": event_start_str,
                        "end_time": event_end_str,
                        "attendees": attendees,
                        "meeting_title": event.get('summary', 'No Title'),
                        "teams_link": teams_link,
                        "teams_meeting_id": teams_id
                    })
                    
            except Exception as e:
                logger.warning(f"[TEAMS] Skipping {email}: {str(e)}")
                continue
        
        logger.info(f"[TEAMS] Found {len(google_meetings)} unique Teams meetings")
        
        if not google_meetings:
            return {
                "platform": "Microsoft Teams",
                "meeting_found": False,
                "meeting_link": None,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "no_show": "NA",  # Meeting link not found in calendar
                "external_participants": [],
                "message": "No Teams meetings found in calendar"
            }
        
        # Step 2: Check Teams call records
        token = get_app_token()
        
        for meeting in google_meetings:
            # Use the actual start and end times for Teams API
            start_iso = start_time.isoformat()
            end_iso = end_time.isoformat()
            
            if start_iso.endswith('+00:00'):
                start_iso = start_iso.replace('+00:00', 'Z')
            elif not start_iso.endswith('Z') and 'T' in start_iso and '+' not in start_iso and '-' not in start_iso.split('T')[1]:
                start_iso += 'Z'
                
            if end_iso.endswith('+00:00'):
                end_iso = end_iso.replace('+00:00', 'Z')
            elif not end_iso.endswith('Z') and 'T' in end_iso and '+' not in end_iso and '-' not in end_iso.split('T')[1]:
                end_iso += 'Z'
            
            call_records = find_meetings_in_timerange(token, start_iso, end_iso)
            
            if not call_records:
                return {
                    "platform": "Microsoft Teams",
                    "meeting_found": False,
                    "meeting_link": meeting["teams_link"],
                    "start_time": meeting["start_time"],
                    "end_time": meeting["end_time"],
                    "no_show": "no data",  # Meeting found in calendar but no call records
                    "external_participants": [],
                    "message": "Meeting found in calendar but no call records found"
                }
            
            # Check each call record
            for record in call_records:
                record_id = record.get("id", "")
                detailed_record = get_call_record_details(token, record_id)
                
                if detailed_record:
                    meeting_data = extract_all_participants(detailed_record)
                    participants = meeting_data['participants']
                    
                    # Check if any @giga.green attendee is in this meeting
                    giga_green_attendees = [email for email in meeting["attendees"] if email.endswith('@giga.green')]
                    
                    giga_found = False
                    for giga_email in giga_green_attendees:
                        found, organizer_name = check_organizer_in_meeting(participants, giga_email, token)
                        if found:
                            giga_found = True
                            break
                    
                    if giga_found:
                        # Check for external participants
                        has_external, external_participants = check_external_participants(participants, token)
                        
                        return {
                            "platform": "Microsoft Teams",
                            "meeting_found": True,
                            "meeting_link": meeting["teams_link"],
                            "start_time": meeting["start_time"],
                            "end_time": meeting["end_time"],
                            "no_show": not has_external,
                            "external_participants": [p['email'] or p['displayName'] for p in external_participants],
                            "external_participants_details": external_participants
                        }
            
            # If we get here, no matching call record was found
            return {
                "platform": "Microsoft Teams",
                "meeting_found": False,
                "meeting_link": meeting["teams_link"],
                "start_time": meeting["start_time"],
                "end_time": meeting["end_time"],
                "no_show": "no data",  # Meeting found but no matching call record with attendees
                "external_participants": [],
                "message": "Meeting found but no matching call record with attendees"
            }
            
    except ValueError as e:
        logger.error(f"[TEAMS] Invalid datetime format: {str(e)}")
        return {
            "platform": "Microsoft Teams",
            "meeting_found": False,
            "meeting_link": None,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "no_show": "incorrect data",
            "error": f"Invalid datetime format: {str(e)}"
        }
    except Exception as e:
        logger.error(f"[TEAMS] Error: {str(e)}")
        return {
            "platform": "Microsoft Teams",
            "meeting_found": False,
            "meeting_link": None,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "no_show": "NA",
            "error": str(e)
        }

# ===================== UNIFIED ENDPOINT =====================

@app.route('/check_meeting_unified', methods=['POST'])
def check_meeting_unified():
    """Unified endpoint to check both Google Meet and Teams meetings"""
    data = request.get_json()
    
    # Validate required inputs
    start_time = data.get("start_time")
    invitee = data.get("invitee", "").lower() if data.get("invitee") else None
    end_time = data.get("end_time")
    
    if not start_time or not invitee:
        return jsonify({"error": "Missing start_time or invitee"}), 400
    
    # Initialize response
    response = {
        "search_criteria": {
            "start_time": start_time,
            "end_time": end_time,
            "invitee": invitee
        },
        "meeting_link": None,
        "platform": None,
        "external_participants": [],
        "no_show": "NA"
    }
    
    # Validate datetime formats
    try:
        # Validate start_time
        parser.isoparse(start_time)
        
        # Validate end_time if provided
        if end_time:
            parser.isoparse(end_time)
    except Exception as e:
        logger.error(f"Invalid datetime format: {str(e)}")
        response["no_show"] = "incorrect data"
        response["error"] = f"Invalid datetime format: {str(e)}"
        return jsonify(response)
    
    try:
        # Check Google Meet (uses only start_time internally)
        logger.info("=" * 50)
        logger.info("Checking Google Meet...")
        google_meet_result = check_google_meet(start_time, invitee)
        
        # Check Microsoft Teams only if end_time is provided
        teams_result = None
        teams_error = None
        if end_time:
            logger.info("=" * 50)
            logger.info("Checking Microsoft Teams...")
            teams_result = check_teams_meeting(start_time, end_time, invitee)
        else:
            teams_error = "end_time not provided - Teams check skipped"
            logger.info(f"[TEAMS] {teams_error}")
        
        # Determine unified no_show status and meeting link
        unified_no_show = "NA"
        meeting_link = None
        platform = None
        external_participants = []
        
        # Check Google Meet result
        if google_meet_result.get("meeting_found"):
            unified_no_show = google_meet_result.get("no_show")
            meeting_link = google_meet_result.get("meeting_link")
            platform = "Google Meet"
            external_participants = google_meet_result.get("external_participants", [])
        
        # Check Teams result (overrides if found)
        if teams_result and teams_result.get("meeting_found"):
            unified_no_show = teams_result.get("no_show")
            meeting_link = teams_result.get("meeting_link")
            external_participants = teams_result.get("external_participants", [])
            # Determine platform based on URL
            if meeting_link and "calendly" in meeting_link.lower():
                platform = "Microsoft Teams (Calendly)"
            else:
                platform = "Microsoft Teams"
        
        # Update response
        response["meeting_link"] = meeting_link
        response["platform"] = platform
        response["external_participants"] = external_participants
        response["no_show"] = unified_no_show
        
        # Add error message if Teams was not checked due to missing end_time
        if teams_error and not google_meet_result.get("meeting_found"):
            response["error"] = teams_error
        
    except Exception as e:
        logger.error(f"Unexpected error during meeting check: {str(e)}")
        response["no_show"] = "incorrect data"
        response["error"] = f"Processing error: {str(e)}"
    
    return jsonify(response)

@app.route('/')
def home():
    return jsonify({
        "message": "Unified Meeting No-Show Detection API",
        "endpoints": {
            "/check_meeting_unified": "POST - Check both Google Meet and Teams meetings"
        },
        "input_format": {
            "start_time": "ISO format datetime (required)",
            "end_time": "ISO format datetime (required for Teams, optional for Google Meet)",
            "invitee": "Email address of the invitee (required)"
        },
        "output_format": {
            "search_criteria": "Input parameters used for search",
            "meeting_link": "URL of the meeting (Google Meet or Teams/Calendly) - null if no meeting found",
            "platform": "Detected platform (Google Meet/Microsoft Teams/null if no meeting found)",
            "external_participants": "List of external participants who joined",
            "no_show": "true/false/'NA'/'no data'/'incorrect data' - Unified no-show status"
        },
        "no_show_values": {
            "true": "Meeting found but no external participants joined",
            "false": "Meeting found and external participants joined",
            "NA": "Meeting link not found in calendar",
            "no data": "Meeting found in calendar but no attendance/reports data available",
            "incorrect data": "Invalid datetime format or processing error"
        },
        "notes": {
            "1": "If end_time is not provided, only Google Meet will be checked",
            "2": "Platform detection: URLs with 'calendly' are marked as 'Microsoft Teams (Calendly)'",
            "3": "If meeting is found in both platforms, Teams takes priority"
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)