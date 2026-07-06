from abc import ABC, abstractmethod

class DeadlineJobInfo(ABC):
    """Abstract base for Deadline job info collection."""
    
    def __init__(self, instance):
        self.data = {}
        
    @abstractmethod
    def process(self, instance):
        pass
