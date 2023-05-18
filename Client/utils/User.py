from dataclasses import dataclass


@dataclass
class User:
    id: int = None
    name: str = None
    password: str = None
    callKey: str = None
    callPassword: str = None
    sessionId: str = None
