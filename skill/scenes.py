import datetime
import inspect
import sys
from dataclasses import asdict
from typing import List

from skill import diary_api, entities, intents, state, texts
from skill.alice import Request, button, image_button, image_list
from skill.dates_transformations import (
    transform_yandex_datetime_value_to_datetime as ya_date_transform,
)
from skill.scenes_util import Scene
from skill.schemas import Homework, PlannedLesson, Student

# region Общие сцены

# класс общая сцена


class GlobalScene(Scene):
    def reply(self, request: Request):
        pass  # Здесь не нужно пока ничего делать

    def handle_global_intents(self, request):
        if intents.HELP in request.intents:
            return HelpMenu()
        if intents.WHAT_CAN_YOU_DO in request.intents:
            return WhatCanDo()
        if intents.GET_HOMEWORK in request.intents:
            return get_scene_for_homework(request)
        if intents.GET_SCHEDULE in request.intents:
            return get_scene_for_schedule(request)
        if intents.RESET in request.intents:
            return Settings_Reset()

    def handle_local_intents(self, request: Request):
        pass  # Здесь не нужно пока ничего делать

    def fallback(self, request: Request):
        if request.session.get(state.NEED_FALLBACK, False):
            text, tts = texts.sorry_and_goodbye()
            return self.make_response(request, text, tts, end_session=True)
        else:
            save_state = {}
            # Сохраним важные состояние
            for save in state.MUST_BE_SAVE:
                if save in request.session:
                    save_state.update({save: request.session[save]})
            save_state[state.NEED_FALLBACK] = True
            text, tts = texts.help_menu_fallback()
            return self.make_response(
                request,
                text,
                tts,
                buttons=HELP,
                state=save_state,
            )


class Welcome(GlobalScene):
    def reply(self, request: Request):
        students = get_all_students_from_request(request)
        if students:
            todo_list = homework_and_schedule(students)
            text, tts = texts.todo_list(todo_list)
            cards = _prepare_cards_todo(todo_list)
            return self.make_response(
                request,
                text,
                tts,
                card=image_list(cards, header=text),
                buttons=DEFAULT_BUTTONS,
            )

        else:
            text, tts = texts.hello()
            buttons = [
                button("Да"),
                button("Что умеешь?"),
            ]
            return self.make_response(
                request,
                text,
                tts,
                buttons=buttons,
            )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return Settings_FirstScene()
        elif intents.REJECT in request.intents:
            return MaybeHelp()


class Goodbye(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.goodbye()
        return self.make_response(request, text, tts, end_session=True)


class SorryAndGoodbye(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.sorry_and_goodbye()
        return self.make_response(request, text, tts, end_session=True)


class HaveMistake(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.mistake()
        return self.make_response(request, text, tts, end_session=True)


class MaybeHelp(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.maybe_you_need_help()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu()
        elif intents.REJECT in request.intents:
            return Goodbye()


class WhatCanDo(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.what_can_i_do()
        return self.make_response(
            request,
            text,
            tts,
            buttons=YES_NO,
            state={},
        )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu()
        if intents.REJECT in request.intents:
            return Welcome()


class HelpMenu(GlobalScene):
    def reply(self, request: Request):
        students = get_all_students_from_request(request)

        text, tts = texts.help_menu_start(students)
        buttons = YES_NO
        if students:
            buttons.append(button("Сбросить настройки"))
        return self.make_response(
            request,
            text,
            tts,
            buttons=buttons,
        )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu_Homework()
        if intents.REJECT in request.intents:
            return HelpMenu_SuggestSchedule()


class HelpMenu_Homework(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.help_menu_homework()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu_Schedule()
        if intents.REJECT in request.intents:
            return HelpMenu_SuggestSpec()


class HelpMenu_SuggestSchedule(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.help_menu_suggest_schedule()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu_Schedule()
        if intents.REJECT in request.intents:
            return HelpMenu_SuggestSpec()


class HelpMenu_Schedule(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.help_menu_schedule()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu_Spec()
        if intents.REJECT in request.intents:
            return Welcome()


class HelpMenu_SuggestSpec(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.help_menu_suggest_spec()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return HelpMenu_Schedule()
        if intents.REJECT in request.intents:
            return HelpMenu_SuggestSpec()


class HelpMenu_Spec(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.help_menu_spec()
        return self.make_response(
            request, text, tts, buttons=[button("Главное меню"), button("Расписание")]
        )

    def handle_local_intents(self, request: Request):
        if intents.MAIN_MENU in request.intents:
            return Welcome()


# endregion


# region settings


# region start


class Settings_FirstScene(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.start_setting()
        return self.make_response(request, text, tts, buttons=HELP)

    def handle_local_intents(self, request: Request):
        if entities.FIO in request.entities_list:
            if not _is_correct_name(request):
                return Settings_FirstScene_FIO()
            if get_student_from_request(request) is None:
                return Settings_GetSchool()
            else:
                return Settings_Duplicate()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.start_setting_fallback())


class Settings_FirstScene_FIO(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.FIO_needs()

        return self.make_response(request, text, tts, buttons=HELP)

    def handle_local_intents(self, request: Request):
        if entities.FIO in request.entities_list:
            if not _is_correct_name(request):
                return Settings_FirstScene_FIO()
            if get_student_from_request(request) is None:
                return Settings_GetSchool()
            else:
                return Settings_Duplicate()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.start_setting_fallback())


class Settings_Duplicate(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.duplicate_name()
        return self.make_response(request, text, tts, buttons=HELP)

    def handle_local_intents(self, request: Request):
        if entities.FIO in request.entities_list:
            if get_student_from_request(request) is None:
                return Settings_GetSchool()
            else:
                return Settings_Duplicate()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.start_setting_fallback())


# endregion

# region school


class Settings_GetSchool(GlobalScene):
    def reply(self, request: Request):
        name = request.entity(entities.FIO)[0]["first_name"].capitalize()
        last_name = request.entity(entities.FIO)[0]["last_name"].capitalize()

        text, tts = texts.what_school(name)
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
            state={state.TEMP_NAME: name, state.TEMP_LAST_NAME: last_name},
        )

    def handle_local_intents(self, request: Request):
        if entities.NUMBER in request.entities_list:
            return Settings_GetClassNumber()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.what_school_fallback())


# endregion

# region class number


class Settings_GetClassNumber(GlobalScene):
    def reply(self, request: Request):
        school_num = request.entity(entities.NUMBER)[0]
        text, tts = texts.what_classnumber()
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
            state={state.TEMP_SCHOOL: school_num},
        )

    def handle_local_intents(self, request: Request):
        if entities.NUMBER in request.entities_list:
            class_num = request.entity(entities.NUMBER)[0]
            if 1 <= class_num <= 11:
                class_letter = request.tokens[-1]
                if "А" <= class_letter.upper() <= "Я":
                    return Settings_Confirm()
                else:
                    return Settings_GetClassLetter()
            else:
                return Settings_IncorrectClassNumber()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.what_classnumber_fallback())


class Settings_IncorrectClassNumber(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.incorrect_classnumber()
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
        )

    def handle_local_intents(self, request: Request):
        if entities.NUMBER in request.entities_list:
            class_num = request.entity(entities.NUMBER)[0]
            if 1 <= class_num <= 11:
                class_letter = request.tokens[-1].upper()
                if len(class_letter) == 1 and "А" <= class_letter <= "Я":
                    return Settings_Confirm()
                else:
                    return Settings_GetClassLetter()
            else:
                return Settings_IncorrectClassNumber()

    def fallback(self, request):
        text, tts = texts.sorry_and_goodbye()
        return self.make_response(request, text, tts, end_session=True)


# endregion

# region class letter


class Settings_GetClassLetter(GlobalScene):
    def reply(self, request: Request):
        class_num = request.entity(entities.NUMBER)[0]
        text, tts = texts.what_classletter()
        return self.make_response(
            request,
            text,
            tts,
            buttons=[
                button("А"),
                button("Б"),
                button("В"),
                button("Г"),
                button("Д"),
                button("Помощь"),
            ],
            state={state.TEMP_CLASS_ID: str(class_num)},
        )

    def handle_local_intents(self, request: Request):
        class_letter = request.tokens[-1].upper()
        if len(class_letter) == 1:
            if "А" <= class_letter <= "Я":
                return Settings_Confirm()
            else:
                return Settings_IncorrectClassLetter()

    def fallback(self, request: Request):
        return global_fallback(self, request, texts.what_classlatter_fallback())


class Settings_IncorrectClassLetter(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.incorrect_classletter()
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
        )

    def handle_local_intents(self, request: Request):
        class_letter = request.tokens[-1]
        if "А" <= class_letter.capitalize() <= "Я":
            return Settings_Confirm()
        else:
            return SorryAndGoodbye()

    def fallback(self, request):
        text, tts = texts.sorry_and_goodbye()
        return self.make_response(request, text, tts, end_session=True)


# endregion


class Settings_Confirm(GlobalScene):
    def reply(self, request: Request):
        prev_session = request.session.get("scene")
        if prev_session == "Settings_GetClassNumber":  # назвали класс полностью
            class_num = request.entity(entities.NUMBER)[0]
            class_letter = request.tokens[-1]
        else:
            class_num = request.session.get(state.TEMP_CLASS_ID)
            class_letter = request.tokens[-1]
        class_id = str(class_num) + class_letter.upper()

        text, tts = texts.confirm_settings(
            request.session.get(state.TEMP_LAST_NAME)
            + " "
            + request.session.get(state.TEMP_NAME),
            request.session.get(state.TEMP_SCHOOL),
            class_id,
        )

        return self.make_response(
            request,
            text,
            tts,
            buttons=YES_NO,
            state={state.TEMP_CLASS_ID: class_id},
        )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return Settings_OneMore()
        elif intents.REJECT in request.intents:
            return Settings_LetsCorrect()


class Settings_OneMore(GlobalScene):
    def reply(self, request: Request):
        students = get_all_students_from_request(request)
        text, tts = texts.one_more_student()

        name = request.session.get(state.TEMP_NAME)
        last_name = request.session.get(state.TEMP_LAST_NAME)
        school = request.session.get(state.TEMP_SCHOOL)
        class_id = request.session.get(state.TEMP_CLASS_ID)
        students.append(Student(name, last_name, school, class_id))
        return self.make_response(
            request,
            text,
            tts,
            buttons=YES_NO,
            state={state.TEMP_NAME: "", state.TEMP_SCHOOL: "", state.TEMP_CLASS_ID: ""},
            user_state={state.STUDENTS: [asdict(student) for student in students]},
        )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return Settings_FirstScene()
        else:
            return Welcome()


# endregion


class Settings_LetsCorrect(Settings_FirstScene):
    def reply(self, request: Request):
        text, tts = texts.discard_settings()
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
            state={state.TEMP_NAME: "", state.TEMP_SCHOOL: "", state.TEMP_CLASS_ID: ""},
        )


class Settings_Reset(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.confirm_reset()
        return self.make_response(request, text, tts, buttons=YES_NO)

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return Settings_ResetConfirm()
        else:
            return Settings_RejectReset()


class Settings_ResetConfirm(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.reset_settings()
        return self.make_response(
            request,
            text,
            tts,
            state={},
            user_state={state.STUDENTS: []},
            end_session=True,
        )


class Settings_RejectReset(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.reject_reset()
        return self.make_response(request, text, tts)


# endregion

# region base scenario


class GetSchedule(GlobalScene):
    def __init__(self, student=None):
        self.student = student

    def reply(self, request: Request):

        context = request.session.get(state.TEMP_CONTEXT, {})
        req_date = get_date_from_request(request)
        if (
            req_date is None
            and context is not None
            and context.get("request_date") is not None
        ):
            req_date = datetime.datetime.strptime(
                context.get("request_date"), "%Y-%m-%d"
            )

        lesson_list = diary_api.get_schedule(
            self.student.school_id,
            self.student.class_id,
            req_date,
        )
        text_title, tts_title = texts.title(self.student, req_date)
        if not lesson_list:
            text, tts = texts.no_schedule()
            return self.make_response(
                request,
                text_title + ". " + text,
                tts_title + " " + tts,
                buttons=[button("Домашнее задание"), button("Главное меню")],
                state={
                    state.TEMP_CONTEXT: {
                        "request_date": req_date
                        if req_date is None
                        else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                        "student": asdict(self.student),
                    }
                },
            )
        else:
            cards = _prepare_cards_lessons(lesson_list)
            text, tts = texts.tell_about_schedule(lesson_list)
            return self.make_response(
                request,
                text_title + ". " + text,
                tts_title + " " + tts,
                card=image_list(cards, header=text_title + ". " + text),
                buttons=[button("Домашнее задание"), button("Главное меню")],
                state={
                    state.TEMP_CONTEXT: {
                        "request_date": req_date
                        if req_date is None
                        else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                        "student": asdict(self.student),
                    }
                },
            )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            context = request.session.get(state.TEMP_CONTEXT, {})
            student = Student(**context.get("student"))
            return GetHomework(student)
        if intents.REJECT in request.intents or intents.MAIN_MENU in request.intents:
            return Welcome()


class GetHomework(GlobalScene):
    def __init__(self, student=None):
        self.student = student

    # Домашнее задание можно спросить указав предмет и дату на какую надо
    def reply(self, request: Request):

        context = request.session.get(state.TEMP_CONTEXT, {})
        req_date = get_date_from_request(request)
        if (
            req_date is None
            and context is not None
            and context.get("request_date") is not None
        ):
            req_date = datetime.datetime.strptime(
                context.get("request_date"), "%Y-%m-%d"
            )

        lessons = get_lessons_from_request(request)
        if not lessons and context is not None and context.get("lessons") is not None:
            lessons = context.get("lessons")

        text_title, tts_title = texts.title(self.student, req_date)

        homework = diary_api.get_homework(
            self.student.school_id, self.student.class_id, req_date, lessons
        )
        if not homework:
            text, tts = texts.no_homework()
            return self.make_response(
                request,
                text_title + ". " + text,
                tts_title + " " + tts,
                buttons=[button("Расписание"), button("Главное меню")],
                state={
                    state.TEMP_CONTEXT: {
                        "request_date": req_date
                        if req_date is None
                        else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                        "lessons": lessons,
                        "student": asdict(self.student),
                    }
                },
            )
        else:
            hw = _split_homework(homework)[0]
            cards = _prepare_cards_hw(hw)
            text, tts = texts.tell_about_homework(hw, len(homework))
            if len(homework) > 3:
                buttons = [
                    button("Назад"),
                    button("Дальше"),
                    button("Главное меню"),
                ]
            else:
                buttons = DEFAULT_BUTTONS
            return self.make_response(
                request,
                text_title + ". " + text,
                tts_title + " " + tts,
                card=image_list(cards, header=text_title + ". " + text),
                buttons=buttons,
                state={
                    state.LIST_HW: [asdict(x) for x in homework],
                    state.TASKS_HW: len(homework),
                    state.SKIP_HW: 0,
                    state.TEMP_CONTEXT: {
                        "request_date": req_date
                        if req_date is None
                        else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                        "lessons": lessons,
                        "student": asdict(self.student),
                    },
                },
            )

    def handle_local_intents(self, request: Request):
        if intents.NEXT in request.intents:
            return TellAboutHomework(1)
        if intents.PREV in request.intents:
            return TellAboutHomework(-1)
        if intents.REPEAT in request.intents:
            return TellAboutHomework(0)
        if intents.CONFIRM in request.intents:
            context = request.session.get(state.TEMP_CONTEXT, {})
            student = Student(**context.get("student"))
            return GetSchedule(student)
        if intents.REJECT in request.intents or intents.MAIN_MENU in request.intents:
            return Welcome()


class TellAboutHomework(GlobalScene):
    def __init__(self, step=0):
        self.__step = step

    def reply(self, request: Request):
        context = request.session.get(state.TEMP_CONTEXT, {})
        req_date = None
        if context is not None and context.get("request_date") is not None:
            req_date = datetime.datetime.strptime(
                context.get("request_date"), "%Y-%m-%d"
            )
        student = Student(**context.get("student"))
        text_title, tts_title = texts.title(student, req_date)

        hw_dict = request.session.get(state.LIST_HW)
        hw = _split_homework([Homework(**x) for x in hw_dict])
        full = request.session.get(state.TASKS_HW)
        step = (request.session.get(state.SKIP_HW) + self.__step) % len(hw)

        cards = _prepare_cards_hw(hw[step])
        text, tts = texts.tell_about_homework(hw[step], full)
        if full > 3:
            buttons = [
                button("Назад"),
                button("Дальше"),
                button("Главное меню"),
            ]
        else:
            buttons = DEFAULT_BUTTONS
        return self.make_response(
            request,
            text_title + ". " + text,
            tts_title + " " + tts,
            card=image_list(cards, header=text_title + ". " + text),
            buttons=buttons,
            state={state.SKIP_HW: step, state.TEMP_CONTEXT: context},
        )

    def handle_local_intents(self, request: Request):
        if intents.NEXT in request.intents:
            return TellAboutHomework(1)
        if intents.PREV in request.intents:
            return TellAboutHomework(-1)
        if intents.REPEAT in request.intents:
            return TellAboutHomework(0)


class NeedSettings(GlobalScene):
    def reply(self, request: Request):
        text, tts = texts.no_settings()
        return self.make_response(
            request,
            text,
            tts,
            buttons=YES_NO,
        )

    def handle_local_intents(self, request: Request):
        if intents.CONFIRM in request.intents:
            return Settings_FirstScene()
        elif intents.REJECT in request.intents:
            return Welcome()


class ChooseStudentSchedule(GlobalScene):
    def __init__(self):
        self.wrong_choice = False

    def reply(self, request: Request):

        req_date = get_date_from_request(request)
        lessons = get_lessons_from_request(request)
        students = get_all_students_from_request(request)
        cards = _prepare_cards_student(students)
        text, tts = texts.choose_schedule(students)
        return self.make_response(
            request,
            text,
            tts,
            card=image_list(cards, header=text),
            state={
                state.TEMP_CONTEXT: {
                    "request_date": req_date
                    if req_date is None
                    else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                    "lessons": lessons,
                }
            },
        )

    def handle_local_intents(self, request: Request):
        if entities.FIO in request.entities_list:
            student = get_student_from_request(request)
            if student is not None:
                return GetSchedule(student)
            else:
                self.wrong_choice = True

    def fallback(self, request):
        return choose_student_fallback(self, request)


class ChooseStudentHomework(GlobalScene):
    def __init__(self):
        self.wrong_choice = False

    def reply(self, request: Request):

        req_date = get_date_from_request(request)
        lessons = get_lessons_from_request(request)
        students = get_all_students_from_request(request)

        cards = _prepare_cards_student(students)
        text, tts = texts.choose_homework(students)
        return self.make_response(
            request,
            text,
            tts,
            card=image_list(cards, header=text),
            state={
                state.TEMP_CONTEXT: {
                    "request_date": req_date
                    if req_date is None
                    else datetime.datetime.strftime(req_date, "%Y-%m-%d"),
                    "lessons": lessons,
                }
            },
        )

    def handle_local_intents(self, request: Request):
        if entities.FIO in request.entities_list:
            student = get_student_from_request(request)
            if student is not None:
                return GetHomework(student)
            else:
                self.wrong_choice = True

    def fallback(self, request):
        return choose_student_fallback(self, request)


# endregion


def global_fallback(self, request: Request, texts_response):
    if request.session.get(state.NEED_FALLBACK, False):
        text, tts = texts.sorry_and_goodbye()
        return self.make_response(request, text, tts, end_session=True)
    else:
        text, tts = texts_response
        return self.make_response(
            request,
            text,
            tts,
            buttons=HELP,
            state={state.NEED_FALLBACK: True},
        )


def choose_student_fallback(self, request: Request):
    if request.session.get(state.NEED_FALLBACK, False):
        text, tts = texts.sorry_and_goodbye()
        return self.make_response(request, text, tts, end_session=True)
    else:
        students = get_all_students_from_request(request)
        cards = _prepare_cards_student(students)
        if self.wrong_choice:
            text, tts = texts.wrong_student_fallback(students)
        else:
            text, tts = texts.choose_student_fallback(students)
        return self.make_response(
            request,
            text,
            tts,
            card=image_list(cards, header=text),
            state={
                state.NEED_FALLBACK: True,
                state.TEMP_CONTEXT: request.session.get(state.TEMP_CONTEXT),
            },
        )


def get_date_from_request(request: Request):
    if entities.DATETIME in request.entities_list:
        ya_date = request.entity(entities.DATETIME)[0]
        ya_date = ya_date_transform(ya_date)
    elif intents.DAY in request.intents:
        day = request.slot(intents.DAY, "Day")
        delta = DAYS.index(day) - datetime.date.today().weekday()
        if delta < 0:
            delta += 7
        ya_date = datetime.datetime.today() + datetime.timedelta(days=delta)
    else:
        ya_date = None

    return ya_date


def get_scene_for_schedule(request: Request):
    students = get_all_students_from_request(request)
    if not students:  # еще не указаны ученики
        return NeedSettings()

    if len(students) > 1:
        context = request.session.get(state.TEMP_CONTEXT)
        if entities.FIO in request.entities_list:
            name = request.entity(entities.FIO)[0]["first_name"].capitalize()
            search = [x for x in students if x == name]
            if not search:
                return ChooseStudentSchedule()
            else:
                student = search[0]
        elif context is not None and "student" in context:
            student = Student(**context.get("student"))
        else:
            return ChooseStudentSchedule()
    else:
        student = students[0]

    return GetSchedule(student)


def get_scene_for_homework(request: Request):
    students = get_all_students_from_request(request)
    if not students:  # еще не указаны ученики
        return NeedSettings()

    if len(students) > 1:
        context = request.session.get(state.TEMP_CONTEXT, {})
        if entities.FIO in request.entities_list:
            name = request.entity(entities.FIO)[0]["first_name"].capitalize()
            search = [x for x in students if x == name]
            if not search:
                return ChooseStudentHomework()
            else:
                student = search[0]
        elif context is not None and "student" in context:
            student = Student(**context.get("student"))
        else:
            return ChooseStudentHomework()
    else:
        student = students[0]

    return GetHomework(student)


def get_all_students_from_request(request: Request) -> List[Student]:
    saved_list = request.user.get(state.STUDENTS, [])
    students = [Student(**s) for s in saved_list]

    return students


def get_student_from_request(request: Request):
    students = get_all_students_from_request(request)
    name = request.entity(entities.FIO)[0]["first_name"].capitalize()
    search = [x for x in students if x == name]
    if not search:
        student = None
    else:
        student = search[0]

    return student


def get_lessons_from_request(request: Request):
    # Выделим предметы. Их может не быть
    lessons = []
    slots = request.intents.get("homework", {}).get("slots", {})
    for i in range(1, 10):
        subject = "subject" + str(i)
        if subject in slots:
            lesson = slots.get(subject).get("value")
            lessons = lessons + entities.subjects.get(lesson)

    return lessons


def homework_and_schedule(students: List[Student]):
    result = {}
    for student in students:
        homework = diary_api.get_homework(student.school_id, student.class_id)
        schedule = diary_api.get_schedule(student.school_id, student.class_id)
        result[student.name] = (len(schedule), len(homework))

    return result


def _split_homework(homework: list):
    n = 3
    if len(homework) <= n:
        list_hw = [homework]
    else:
        list_hw = [homework[i : i + n] for i in range(0, len(homework), n)]
        if len(list_hw) > 1 and len(list_hw[-1]) == 1:
            # последний урок из ПРЕДПОСЛЕДНЕЙ групп становится ПЕРВЫМ в последней группе
            list_hw[-1].insert(0, list_hw[-2].pop(2))
    return list_hw


def _prepare_cards_hw(homeworks: List[Homework]):
    return [
        image_button(
            title=x.lesson.capitalize(), description=x.task, image_id=x.link_url
        )
        for x in homeworks
    ]


def _prepare_cards_lessons(lessons: List[PlannedLesson]):
    return [
        image_button(
            title=x.name.capitalize(), description=x.duration, image_id=x.link_url
        )
        for x in lessons
    ]


def _prepare_cards_student(students: List[Student]):
    return [
        image_button(title=x.name.capitalize(), button_text=x.name.capitalize())
        for x in students
    ]


def _prepare_cards_todo(todo):
    return [
        image_button(
            title=name.capitalize(),
            description=f"Доманее задание: {task[0]}. Уроков: {task[1]}",
        )
        for name, task in todo.items()
    ]


def _is_correct_name(request: Request):
    return (
        request.entity(entities.FIO)[0].get("first_name") is not None
        and request.entity(entities.FIO)[0].get("last_name") is not None
    )


def _list_scenes():
    current_module = sys.modules[__name__]
    scenes = []
    for name, obj in inspect.getmembers(current_module):
        if inspect.isclass(obj) and issubclass(obj, Scene):
            scenes.append(obj)
    return scenes


SCENES = {scene.id(): scene for scene in _list_scenes()}

DEFAULT_SCENE = Welcome
YES_NO = [button("Да"), button("Нет")]
HELP = [button("Помощь")]
DEFAULT_BUTTONS = [
    button("Домашнее задание"),
    button("Расписание"),
]
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
