"""
Optimizely (Zaius) API client for RunSignup connector.

Provides functions to post profile updates and events to Optimizely.
"""

import os
import requests
from typing import Dict, Optional, Tuple


OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "").strip()
OPTIMIZELY_EVENTS_ENDPOINT = "https://api.zaius.com/v3/events"
OPTIMIZELY_PROFILES_ENDPOINT = "https://api.zaius.com/v3/profiles"


def _get_headers() -> Dict[str, str]:
    """Get headers for Optimizely API requests."""
    if not OPTIMIZELY_API_TOKEN:
        raise ValueError(
            "OPTIMIZELY_API_TOKEN environment variable is not set. "
            "Please set it in your environment or GitHub Secrets."
        )
    return {
        "x-api-key": OPTIMIZELY_API_TOKEN,
        "Content-Type": "application/json"
    }


def post_profile(email: str, attrs: Dict, list_id: Optional[str] = None) -> Tuple[int, str]:
    """
    Post a profile update to Optimizely.
    
    Args:
        email: Email address of the profile
        attrs: Dictionary of profile attributes
        list_id: Optional Optimizely list ID to subscribe the contact to
        
    Returns:
        Tuple of (status_code, response_text)
        
    Raises:
        ValueError: If OPTIMIZELY_API_TOKEN is missing
        requests.RequestException: On network errors
    """
    headers = _get_headers()
    
    payload = {
        "identifiers": {
            "email": email
        },
        "attributes": attrs
    }
    
    # Add list subscription if list_id is provided
    if list_id:
        payload["lists"] = [{"id": list_id, "subscribe": True}]
    
    try:
        response = requests.post(
            OPTIMIZELY_PROFILES_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"Network error posting profile for {email}: {e}"
        )


def post_event(
    email: str,
    event_name: str,
    properties: Dict,
    timestamp_iso: Optional[str] = None,
    list_id: Optional[str] = None
) -> Tuple[int, str]:
    """
    Post an event to Optimizely.
    
    Args:
        email: Email address of the user
        event_name: Name of the event (e.g., "registration")
        properties: Dictionary of event properties
        timestamp_iso: ISO 8601 timestamp string (optional, defaults to now)
        list_id: Optional Optimizely list ID to subscribe the contact to
        
    Returns:
        Tuple of (status_code, response_text)
        
    Raises:
        ValueError: If OPTIMIZELY_API_TOKEN is missing
        requests.RequestException: On network errors
    """
    from datetime import datetime, timezone
    
    headers = _get_headers()
    
    if timestamp_iso is None:
        timestamp_iso = datetime.now(timezone.utc).isoformat()
    
    payload = {
        "type": event_name,
        "timestamp": timestamp_iso,
        "identifiers": {
            "email": email
        },
        "properties": properties
    }
    
    # Add list subscription if list_id is provided
    if list_id:
        payload["lists"] = [{"id": list_id, "subscribe": True}]
    
    try:
        response = requests.post(
            OPTIMIZELY_EVENTS_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"Network error posting event {event_name} for {email}: {e}"
        )

