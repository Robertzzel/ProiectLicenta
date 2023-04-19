from dataclasses import dataclass


@dataclass
class User:
    id: int
    name: str
    password: str
    callKey: str
    callPassword: str
    sessionId: str = None
