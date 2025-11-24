"""
Optimizely (Zaius) API client for RunSignup connector.

Provides functions to post profile updates and events to Optimizely.
"""

import os
import time
import requests
import json
from typing import Dict, Optional, Tuple, Literal


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


def get_profile(email: str) -> Optional[Dict]:
    """
    Fetch an existing profile from Optimizely by email.
    
    Uses the /v3/profiles endpoint with email identifier.
    
    Args:
        email: Email address of the profile
        
    Returns:
        Profile data as dict if found, None if not found or on error
        
    Raises:
        ValueError: If OPTIMIZELY_API_TOKEN is missing
        requests.RequestException: On network errors
    """
    headers = _get_headers()
    
    # Use GET /v3/profiles with email identifier
    # The API accepts identifiers as query parameters or in the request body
    # We'll use a POST request with identifiers in the body to fetch the profile
    payload = {
        "identifiers": {
            "email": email
        }
    }
    
    # Retry logic for network errors and 5xx status codes
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Try GET with email as query parameter first
            # If that doesn't work, we'll try POST with identifiers in body
            response = requests.get(
                OPTIMIZELY_PROFILES_ENDPOINT,
                headers=headers,
                params={"email": email},
                timeout=TIMEOUT
            )
            
            # If GET doesn't work (405 Method Not Allowed), try POST
            if response.status_code == 405:
                response = requests.post(
                    OPTIMIZELY_PROFILES_ENDPOINT,
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
                    return None  # Return None on persistent server errors
            
            # 404 means profile doesn't exist - that's fine, return None
            if response.status_code == 404:
                return None
            
            # 200/202 means profile found
            if response.status_code in (200, 202):
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return None
            
            # Other 4xx errors - profile might not exist or invalid request
            if response.status_code >= 400:
                return None
            
            return None
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * attempt
                time.sleep(delay)
                continue
            else:
                # On network error, return None (don't fail the whole process)
                return None
        except requests.exceptions.RequestException:
            # On other request exceptions, return None
            return None
    
    return None


def subscribe_to_list(email: str, list_id: str) -> Tuple[int, str]:
    """
    Subscribe a profile to an Optimizely email list.
    
    Uses the /v3/events endpoint with a list_subscribe event type.
    This respects unsubscribe state - won't re-subscribe if they've opted out.
    
    Args:
        email: Email address of the profile
        list_id: Optimizely list ID to subscribe to
        
    Returns:
        Tuple of (status_code, response_text)
        
    Raises:
        ValueError: If OPTIMIZELY_API_TOKEN is missing
        requests.RequestException: On network errors
    """
    from datetime import datetime, timezone
    
    headers = _get_headers()
    
    # Use events endpoint with list_subscribe event type
    payload = {
        "type": "list_subscribe",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "identifiers": {
            "email": email
        },
        "properties": {
            "list_id": list_id
        }
    }
    
    # Retry logic for network errors and 5xx status codes
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPTIMIZELY_EVENTS_ENDPOINT,  # Use events endpoint for list subscriptions
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
            
            # Log non-2xx responses for debugging
            if response.status_code not in (200, 202, 204):
                # Include response body in error for debugging
                try:
                    response_json = response.json()
                    error_msg = f"Status {response.status_code}: {json.dumps(response_json)}"
                except:
                    error_msg = f"Status {response.status_code}: {response.text[:200]}"
                return response.status_code, error_msg
            
            # Return immediately for 2xx (don't retry client errors)
            return response.status_code, response.text
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAY * attempt
                time.sleep(delay)
                continue
            else:
                raise requests.exceptions.RequestException(
                    f"Network error subscribing {email} to list {list_id} after {MAX_RETRIES} attempts: {e}"
                )
        except requests.exceptions.RequestException as e:
            # Don't retry on other request exceptions (4xx errors, etc.)
            raise requests.exceptions.RequestException(
                f"Network error subscribing {email} to list {list_id}: {e}"
            )
    
    # Should never reach here, but just in case
    raise requests.exceptions.RequestException(
        f"Failed to subscribe {email} to list {list_id} after {MAX_RETRIES} attempts"
    )


def check_subscription_status(email: str, list_id: str) -> Optional[Dict]:
    """
    Check if a profile is subscribed to a specific list.
    
    Fetches the profile and returns subscription information for the given list_id.
    
    Args:
        email: Email address of the profile
        list_id: Optimizely list ID to check
        
    Returns:
        Dictionary with subscription info if found, None if profile doesn't exist
        Format: {"subscribed": bool, "list_id": str, "status": str}
    """
    profile = get_profile(email)
    if not profile:
        return None
    
    subscriptions = profile.get("subscriptions", [])
    for sub in subscriptions:
        if sub.get("list_id") == list_id:
            return {
                "subscribed": sub.get("subscribed", False),
                "list_id": list_id,
                "status": "subscribed" if sub.get("subscribed") else "unsubscribed",
                "profile_suppressed": profile.get("suppressed", False),
                "profile_unsubscribed": profile.get("unsubscribed", False)
            }
    
    # List subscription not found
    return {
        "subscribed": False,
        "list_id": list_id,
        "status": "not_found",
        "profile_suppressed": profile.get("suppressed", False),
        "profile_unsubscribed": profile.get("unsubscribed", False)
    }


def upsert_profile_with_subscription(
    email: str,
    profile_attrs: Dict,
    list_id: str
) -> Tuple[Literal["created", "updated"], str, bool]:
    """
    Upsert a profile and handle list subscription idempotently.
    
    This function:
    1. Fetches the existing profile by email
    2. If profile doesn't exist: creates it and subscribes to list_id
    3. If profile exists: checks subscription status
       - If unsubscribed or globally suppressed: does NOT change it
       - If missing or pending: sets to subscribed
    
    Args:
        email: Email address of the profile
        profile_attrs: Dictionary of profile attributes to update
        list_id: Optimizely list ID to subscribe to (if not unsubscribed)
        
    Returns:
        Tuple of (action: "created" | "updated", status_message: str, was_subscribed: bool)
        - action: Whether we created a new profile or updated existing
        - status_message: Human-readable status message
        - was_subscribed: True if we subscribed (or already subscribed), False if kept unsubscribed
        
    Raises:
        ValueError: If OPTIMIZELY_API_TOKEN is missing
        requests.RequestException: On network errors
    """
    # Fetch existing profile
    existing_profile = get_profile(email)
    
    if existing_profile is None:
        # Profile doesn't exist - create it and subscribe
        # Include list_id in post_profile to subscribe in the same call (like RICS connector)
        status_code, response_text = post_profile(email, profile_attrs, list_id)
        
        if status_code not in (200, 202):
            raise requests.exceptions.RequestException(
                f"Failed to create profile for {email}: {status_code} - {response_text[:200]}"
            )
        
        # Subscription is included in the customer_update event, so we're done
        return ("created", f"Created profile and subscribed to list {list_id}", True)
    
    # Profile exists - check subscription status
    subscriptions = existing_profile.get("subscriptions", [])
    
    # Find subscription for this list_id
    list_subscription = None
    for sub in subscriptions:
        if sub.get("list_id") == list_id:
            list_subscription = sub
            break
    
    # Check for global suppression first (takes precedence)
    if existing_profile.get("suppressed", False) or existing_profile.get("unsubscribed", False):
        # Globally suppressed - don't subscribe
        status_code, response_text = post_profile(email, profile_attrs)
        if status_code not in (200, 202):
            raise requests.exceptions.RequestException(
                f"Failed to update profile for {email}: {status_code} - {response_text[:200]}"
            )
        return ("updated", f"Updated profile but kept globally suppressed/unsubscribed", False)
    
    # Check if explicitly unsubscribed from this specific list
    if list_subscription:
        sub_status = list_subscription.get("subscribed")
        # If explicitly unsubscribed (False), don't change it
        if sub_status is False:
            # Still update profile attributes, but don't include list_id (don't change subscription)
            status_code, response_text = post_profile(email, profile_attrs, list_id=None)
            if status_code not in (200, 202):
                raise requests.exceptions.RequestException(
                    f"Failed to update profile for {email}: {status_code} - {response_text[:200]}"
                )
            return ("updated", f"Updated profile but kept unsubscribed from list {list_id}", False)
        
        # If already subscribed (True), update profile but don't need to re-subscribe
        if sub_status is True:
            # Update profile without list_id since already subscribed (avoids duplicate subscription)
            status_code, response_text = post_profile(email, profile_attrs, list_id=None)
            if status_code not in (200, 202):
                raise requests.exceptions.RequestException(
                    f"Failed to update profile for {email}: {status_code} - {response_text[:200]}"
                )
            return ("updated", f"Updated profile, already subscribed to list {list_id}", True)
    
    # Subscription is missing, None, or pending - subscribe them
    # Include list_id in post_profile to subscribe in the same call (like RICS connector)
    status_code, response_text = post_profile(email, profile_attrs, list_id)
    if status_code not in (200, 202):
        raise requests.exceptions.RequestException(
            f"Failed to update profile for {email}: {status_code} - {response_text[:200]}"
        )
    
    # Subscription is included in the customer_update event, so we're done
    return ("updated", f"Updated profile and subscribed to list {list_id}", True)


def post_profile(email: str, attrs: Dict, list_id: Optional[str] = None) -> Tuple[int, str]:
    """
    Post a profile update to Optimizely using the events endpoint.
    
    Uses the /v3/events endpoint with type "customer_update" (same as RICS connector)
    instead of /v3/profiles endpoint, as this is the working pattern in the codebase.
    
    If list_id is provided, includes it in the "lists" field to subscribe the profile
    to the list (same pattern as RICS connector).
    
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
    
    # Include list subscription in the event payload (same pattern as RICS connector)
    # This is more reliable than using a separate list_subscribe event
    if list_id:
        payload["lists"] = [{"id": list_id, "subscribe": True}]
    
    # Debug logging for subscription attempts
    if list_id:
        import json as json_module
        print(f"🔍 DEBUG: Posting profile with list subscription:")
        print(f"   Email: {email}")
        print(f"   List ID: {list_id}")
        print(f"   Payload (lists field): {json_module.dumps(payload.get('lists', []), indent=2)}")
    
    # Retry logic for network errors and 5xx status codes
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPTIMIZELY_EVENTS_ENDPOINT,  # Use events endpoint, not profiles
                headers=headers,
                json=payload,
                timeout=TIMEOUT
            )
            
            # Debug logging for responses
            if list_id:
                print(f"🔍 DEBUG: API Response for subscription:")
                print(f"   Status: {response.status_code}")
                try:
                    response_json = response.json()
                    print(f"   Response body: {json_module.dumps(response_json, indent=2)[:500]}")
                except:
                    print(f"   Response text: {response.text[:500]}")
            
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
    
    # Note: List subscription is handled separately via subscribe_to_list()
    # The events endpoint doesn't reliably handle list subscriptions
    
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

