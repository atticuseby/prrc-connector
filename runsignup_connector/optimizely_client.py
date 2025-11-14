"""
Optimizely (Zaius) API client for RunSignup connector.

Provides functions to post profile updates and events to Optimizely.
"""

import os
import time
import requests
from typing import Dict, Optional, Tuple


OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "").strip()
OPTIMIZELY_EVENTS_ENDPOINT = "https://api.zaius.com/v3/events"
OPTIMIZELY_PROFILES_ENDPOINT = "https://api.zaius.com/v3/profiles"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
TIMEOUT = 30  # Increased from 10 to 30 seconds


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
    Post a profile update to Optimizely using the events endpoint.
    
    Uses the /v3/events endpoint with type "customer_update" (same as RICS connector)
    instead of /v3/profiles endpoint, as this is the working pattern in the codebase.
    
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
    from datetime import datetime, timezone
    
    headers = _get_headers()
    
    # Use events endpoint with customer_update type (same pattern as RICS connector)
    payload = {
        "type": "customer_update",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "identifiers": {
            "email": email
        },
        "properties": attrs  # Use "properties" not "attributes" for events endpoint
    }
    
    # Add list subscription if list_id is provided
    if list_id:
        payload["lists"] = [{"id": list_id, "subscribe": True}]
    
    # Retry logic for network errors and 5xx status codes
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPTIMIZELY_EVENTS_ENDPOINT,  # Use events endpoint, not profiles
                headers=headers,
                json=payload,
                timeout=TIMEOUT
            )
            
            # Retry on 5xx errors (server errors)
            if response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * attempt
                    time.sleep(delay)
                    continue
                else:
                    return response.status_code, response.text
            
            # Return immediately for 2xx, 4xx (don't retry client errors)
            return response.status_code, response.text
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * attempt
                time.sleep(delay)
                continue
            else:
                raise requests.exceptions.RequestException(
                    f"Network error posting profile for {email} after {MAX_RETRIES} attempts: {e}"
                )
        except requests.exceptions.RequestException as e:
            # Don't retry on other request exceptions (4xx errors, etc.)
            raise requests.exceptions.RequestException(
                f"Network error posting profile for {email}: {e}"
            )
    
    # Should never reach here, but just in case
    raise requests.exceptions.RequestException(
        f"Failed to post profile for {email} after {MAX_RETRIES} attempts"
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
    
    # Retry logic for network errors and 5xx status codes
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPTIMIZELY_EVENTS_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=TIMEOUT
            )
            
            # Retry on 5xx errors (server errors)
            if response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * attempt
                    time.sleep(delay)
                    continue
                else:
                    return response.status_code, response.text
            
            # Return immediately for 2xx, 4xx (don't retry client errors)
            return response.status_code, response.text
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * attempt
                time.sleep(delay)
                continue
            else:
                raise requests.exceptions.RequestException(
                    f"Network error posting event {event_name} for {email} after {MAX_RETRIES} attempts: {e}"
                )
        except requests.exceptions.RequestException as e:
            # Don't retry on other request exceptions (4xx errors, etc.)
            raise requests.exceptions.RequestException(
                f"Network error posting event {event_name} for {email}: {e}"
            )
    
    # Should never reach here, but just in case
    raise requests.exceptions.RequestException(
        f"Failed to post event {event_name} for {email} after {MAX_RETRIES} attempts"
    )

