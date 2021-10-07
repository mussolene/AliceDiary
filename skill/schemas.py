from dataclasses import dataclass
from datetime import time

from skill.entities import image_ids, subjects


@dataclass
class Student:
    name: str
    last_name: str
    school_id: str
    class_id: str

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other

        return False

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


@dataclass
class PlannedLesson:
    name: str
    start: time
    end: time

    @property
    def start_time(self):
        return time.strftime(self.start, "%H:%M")

    @property
    def end_time(self):
        return time.strftime(self.end, "%H:%M")

    @property
    def duration(self):
        result = ""
        if self.start and self.end:
            result = f"{self.start_time} - {self.end_time}"
        return result

    @property
    def link_url(self):
        result = ""
        for key, value in subjects.items():
            name_subject = ""
            if self.name in value:
                name_subject = key
            if not image_ids.get(name_subject) is None:
                result = image_ids[name_subject]

        return result


@dataclass
class Homework:
    lesson: str
    task: str

    @property
    def link_url(self):
        result = ""
        for key, value in subjects.items():
            name_subject = ""
            if self.lesson in value:
                name_subject = key
            if not image_ids.get(name_subject) is None:
                result = image_ids[name_subject]

        return result
