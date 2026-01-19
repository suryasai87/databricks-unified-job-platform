"""
Genie Router - AI Assistant endpoints for natural language queries
"""
import os
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

router = APIRouter()


class GenieSpace(BaseModel):
    id: str
    name: str
    description: Optional[str]


class ConversationRequest(BaseModel):
    space_id: str
    initial_message: Optional[str] = None


class MessageRequest(BaseModel):
    content: str


class GenieMessage(BaseModel):
    role: str
    content: str
    sql: Optional[str] = None
    results: Optional[List[dict]] = None


def get_workspace_client():
    """Get workspace client from data layer."""
    from app import data_layer
    if not data_layer or not data_layer._workspace_client:
        raise HTTPException(status_code=503, detail="Workspace client not initialized")
    return data_layer._workspace_client


@router.get("/spaces", response_model=List[GenieSpace])
async def get_genie_spaces():
    """Get available Genie Spaces."""
    client = get_workspace_client()

    try:
        # Use Genie API to list spaces
        response = client.api_client.do("GET", "/api/2.0/genie/spaces")

        if isinstance(response, dict) and "spaces" in response:
            return [
                GenieSpace(
                    id=space.get("id"),
                    name=space.get("name"),
                    description=space.get("description"),
                )
                for space in response.get("spaces", [])
            ]
        return []

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list Genie Spaces: {str(e)}")


@router.post("/conversations")
async def start_conversation(request: ConversationRequest):
    """Start a new Genie conversation."""
    client = get_workspace_client()

    try:
        # Create new conversation
        response = client.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{request.space_id}/start-conversation",
            body={"content": request.initial_message} if request.initial_message else {},
        )

        if isinstance(response, dict):
            return {
                "conversation_id": response.get("conversation_id"),
                "message_id": response.get("message_id"),
                "status": response.get("status", "started"),
            }

        raise HTTPException(status_code=500, detail="Invalid response from Genie API")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start conversation: {str(e)}")


@router.post("/conversations/{conversation_id}/messages")
async def send_message(conversation_id: str, request: MessageRequest):
    """Send a message to a Genie conversation and get response."""
    client = get_workspace_client()
    space_id = os.getenv("GENIE_SPACE_ID")

    if not space_id:
        raise HTTPException(status_code=400, detail="GENIE_SPACE_ID not configured")

    try:
        # Send message
        response = client.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
            body={"content": request.content},
        )

        if isinstance(response, dict):
            # Poll for result if needed
            message_id = response.get("message_id")
            status = response.get("status")

            # If still processing, poll for completion
            if status in ["EXECUTING_QUERY", "PENDING"]:
                import time
                for _ in range(30):  # Max 30 seconds
                    time.sleep(1)
                    poll_response = client.api_client.do(
                        "GET",
                        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                    )
                    if isinstance(poll_response, dict):
                        status = poll_response.get("status")
                        if status in ["COMPLETED", "FAILED"]:
                            response = poll_response
                            break

            # Extract result
            result = {
                "message_id": response.get("message_id"),
                "status": response.get("status"),
                "content": response.get("attachments", [{}])[0].get("text", {}).get("content", ""),
            }

            # Check for SQL query
            attachments = response.get("attachments", [])
            for attachment in attachments:
                if attachment.get("query"):
                    result["sql"] = attachment.get("query", {}).get("query")
                    result["results"] = attachment.get("query", {}).get("result", {}).get("data_array", [])

            return result

        raise HTTPException(status_code=500, detail="Invalid response from Genie API")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")


@router.get("/conversations/{conversation_id}/history")
async def get_conversation_history(conversation_id: str):
    """Get conversation history."""
    client = get_workspace_client()
    space_id = os.getenv("GENIE_SPACE_ID")

    if not space_id:
        raise HTTPException(status_code=400, detail="GENIE_SPACE_ID not configured")

    try:
        response = client.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}",
        )

        if isinstance(response, dict):
            messages = []
            for msg in response.get("messages", []):
                message = GenieMessage(
                    role=msg.get("role", "assistant"),
                    content=msg.get("content", ""),
                )
                # Extract SQL if present
                for attachment in msg.get("attachments", []):
                    if attachment.get("query"):
                        message.sql = attachment.get("query", {}).get("query")
                messages.append(message)

            return {"conversation_id": conversation_id, "messages": messages}

        raise HTTPException(status_code=500, detail="Invalid response from Genie API")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")


@router.get("/suggested-questions")
async def get_suggested_questions():
    """Get suggested questions for the AI assistant."""
    return {
        "categories": [
            {
                "name": "Job Monitoring",
                "questions": [
                    "Show me all failed jobs in the last 24 hours",
                    "Which jobs are running longer than usual?",
                    "What is the success rate for each job this week?",
                    "List the top 10 longest running jobs",
                ],
            },
            {
                "name": "Cost Analysis",
                "questions": [
                    "What is the total cost by department this month?",
                    "Which projects have the highest serverless compute costs?",
                    "Show me the daily cost trend for the last 30 days",
                    "What percentage of costs are untagged?",
                ],
            },
            {
                "name": "Performance",
                "questions": [
                    "Which jobs have the most retries?",
                    "Show duration percentiles for all jobs",
                    "Are there any anomalies in job execution times?",
                    "What is the SLA compliance rate?",
                ],
            },
            {
                "name": "ADF Integration",
                "questions": [
                    "Show tag correlation rate for ADF pipelines",
                    "Which ADF pipelines have the most job runs?",
                    "List unmatched serverless runs",
                    "What is the cost by ADF pipeline?",
                ],
            },
        ],
    }
