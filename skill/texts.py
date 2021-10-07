import datetime
import locale
import random
from typing import List

import pymorphy2

from skill.schemas import Homework, PlannedLesson, Student

morph = pymorphy2.MorphAnalyzer()
locale.setlocale(locale.LC_TIME, ("RU", "UTF8"))  # the ru locale is installed


def __inflect(word, case):
    return " ".join([morph.parse(x)[-1].inflect(case).word for x in word.split(" ")])


def hello():

    text = (
        "Привет! Это цифровой дневник.\n"
        "Я могу подсказать расписание уроков или напомнить, что задали домой.\n"
        "Но для начала нужно выполнить простые настройки. Готовы продолжить?"
    )
    tts = text

    return text, tts


def todo_list(todo):
    result = ["Привет! Это Цифровой дневник.", "Вот список дел на сегодня."]
    for name, task in todo.items():
        if task[0] == 0 and task[1] == 0:
            result.append(__all_empty(name))
        if task[0] != 0 and task[1] == 0:
            result.append(__only_homework(name, task[0]))
        if task[0] == 0 and task[1] != 0:
            result.append(__only_schedule(name, task[1]))
        if task[0] != 0 and task[1] != 0:
            result.append(__full_work(name, task[0], task[1]))

    result.append("Чтобы проверить домашнее задание, спросите меня. Что задали домой?")
    text = "Список дел на сегодня"
    tts = " ".join(result)

    return text, tts


def __all_empty(name):
    options = [
        f"У {__inflect(name, {'gent'})} нет домашнего задания и расписание пустое.",
        f"{__inflect(name, {'datv'})} повезло: ни домашнего задания, ни уроков.",
    ]

    return random.choice(options)


def __only_homework(name, homework):
    options = [
        f"У {__inflect(name, {'gent'})} только домашняя работа, "
        f"{make_agree_with_number('задание', homework)}.",
        f"{__inflect(name, {'datv'})} надо сделать домашнее задание. "
        f"{make_agree_with_number('задача', homework)}.",
    ]

    return random.choice(options)


def __only_schedule(name, lessons):
    options = [
        f"У {__inflect(name, {'gent'})} нет домашнего задания."
        f"По расписанию {make_agree_with_number('урок', lessons)}",
        f"{__inflect(name, {'datv'})} домой ничего не задали. "
        f"Сегодня будет {make_agree_with_number('урок', lessons)}",
    ]

    return random.choice(options)


def __full_work(name, homework, lessons):
    options = [
        f"{__inflect(name, {'datv'})} домой задали "
        f"{make_agree_with_number('задание', homework)} "
        f"и по расписанию {make_agree_with_number('урок', lessons)}.",
        f"{__inflect(name, {'datv'})}  надо сделать "
        f"{make_agree_with_number('задача', homework)} "
        f"и сегодня у него {make_agree_with_number('урок', lessons)}.",
    ]

    return random.choice(options)


def mistake():
    text = (
        "Прошу прощения, в навыке возникла непредвиденная ошибка.\n"
        "Мы её обязательно исправим. Возвращайтесь чуть позже."
    )

    return text, text


def sorry_and_goodbye():
    text = (
        "Прошу прощения, я очень стараюсь вас понять.\n"
        "Но пока не получается. Возможно, мне стоит отдохнуть. Возвращайтесь позже.\n"
        "До свидания!"
    )

    tts = text
    return text, tts


def maybe_you_need_help():
    text = "Попробую вам помочь. Рассказать, что я умею?"

    tts = text
    return text, tts


def goodbye():
    text = "Возвращайтесь в любое время. До свидания!"
    tts = "<speaker audio='alice-sounds-game-loss-3.opus'>" + text
    return text, tts


# region Меню помощи


def what_can_i_do():
    text = (
        "Этот навык помогает работать с дневником.\n"
        "Я могу рассказать что задали делать дома\n"
        "или подсказать расписание уроков.\n"
        "Пока я работаю в тестовом режиме.\n"
        "Поэтому пока доступна только школа № 5, класс 1 А\n"
        "Но скоро я заработаю в полную силу.\n"
        "Рассказать подробнее, что я умею?"
    )

    tts = (
        "Этот навык помогает работать с дневником.\n"
        "Я могу рассказать что задали делать дома\n"
        "или подсказать расписание уроков.\n"
        "Пока я работаю в тестовом режиме.\n"
        "Поэтому пока доступна только школа номер 5, класс 1 sil<[100]> А\n"
        "Но скоро я заработаю в полную силу.\n"
        "Рассказать подробнее, что я умею?"
    )

    return text, tts


def help_menu_start(students):
    text = (
        "С помощью этого навыка Вы сможете узнать "
        "домашнее задание или расписание в школе.\n"
    )

    if not students:
        text += (
            "Пока у Вас не настроено ни одного ученика.\n"
            "Это можно будет сделать из основного меню\n"
        )
        tts = text
    else:
        text += "Сейчас настроены следующие ученики:\n"
        tts = text
        text += "\n".join(
            [
                f"{student.name} учится в школе "
                f"№{student.school_id} в {student.class_id}."
                for student in students
            ]
        )
        tts += "\n".join(
            [
                f"{student.name} учится в школе номер "
                f"{student.school_id} в {student.class_id} классе."
                for student in students
            ]
        )
        text += (
            "\n"
            "Чтобы изменить настройки нажмите "
            "Сбрось настройки и перезапустите навык.\n"
        )
        tts += (
            "Чтобы изменить настройки скажите "
            "Сбрось настройки и перезапуст+ите навык."
        )

    text += "Хотите расскажу как получить домашнее задание?"
    tts += "Хотите расскажу как получить домашнее задание?"

    return text, tts


def help_menu_homework():
    text = (
        "Чтобы узнать, что задали, скажите:\n"
        '"Какое домашнее задание?"\n'
        "Или, если хотите уточнить предмет:\n"
        '"Что задали на завтра по математике?\n'
        "Если у Вас указано несколько учеников, можете добавить имя:\n"
        '"Что Алисе задали по риторике на послезавтра"\n'
        "Теперь Вы знаете как получить домашнюю работу."
        "А еще я могу подсказать расписание. Рассказать, как?"
    )
    tts = text

    return text, tts


def help_menu_suggest_schedule():
    text = "Ладно. А еще я могу подсказать расписание. Рассказать, как?"
    tts = text

    return text, tts


def help_menu_schedule():
    text = (
        "Чтобы узнать расписание скажите:\n"
        '"Подскажи расписание?"\n'
        "Или, если хотите на определенный день:\n"
        '"Какие уроки завтра?"\n'
        "Если у Вас указано несколько учеников, можете добавить имя:\n"
        '"Какое расписание у Алисы послезавтра?"\n'
        "Теперь Вы знаете как узнать расписание учеников."
        "Хотите расскажу о моих специальных возможностях?"
    )
    tts = text

    return text, tts


def help_menu_suggest_spec():
    text = "Ладно. Хотите расскажу о моих специальных возможностях?"
    tts = text

    return text, tts


def help_menu_spec():
    text = (
        "У меня есть несколько режимов запуска:\n"
        "Можете сказать:\n"
        '"Алиса, запусти навык Цифровой дневник"\n'
        "И попадете в это приложение:\n"
        "А можете сказать:\n"
        '"Алиса, спроси у Цифрового дневника что задали?"\n'
        "Тогда я сразу отвечу на Ваш вопрос.\n"
        "Еще я часть Утреннего шоу. Напомню, какие уроки будут. \n"
        "Теперь вы знаете как пользоваться навыком. \n"
        'Скажите "Главное меню" или попробуйте узнать домашнее задание.'
    )
    tts = text

    return text, tts


def help_menu_fallback():
    text = (
        "Извините, я вас не поняла. Пожалуйста, повторите.\n"
        'Скажите "Помощь", чтобы узнать как работает навык.'
    )

    return text, text


# endregion

# region Расписание уроков


def no_schedule():

    text = "По расписанию ничего нет."
    tts = (
        "По расписанию ничего нет."
        "Можно провести этот день с пользой. Погулять или заняться физкультурой."
        "Хотите узнать домашнее задание?"
    )

    return text, tts


def tell_about_schedule(list_of_lessons: List[PlannedLesson]):
    text = "Уроков: " + str(len(list_of_lessons))
    tts = "Всего " + __how_many_lessons(len(list_of_lessons))
    for lesson in list_of_lessons:
        tts += __tell_about_lesson(lesson.name, lesson.start_time)
    tts += (
        "sil<[200]> Скажите Повтори, если хотите послушать еще раз."
        "Хотите узнать домашнее задание?"
    )
    return text, tts


def __tell_about_lesson(lesson: str, time: str):
    return f"sil<[200]> {lesson} начинается в {time}"


def __how_many_lessons(n: int) -> str:
    return make_agree_with_number("урок", n)


def make_agree_with_number(word: str, num: int) -> str:
    return str(num) + " " + morph.parse(word)[0].make_agree_with_number(num).word


# endregion

# region Домашняя работа


def no_homework():
    text = (
        "Ничего не задали. Можно отдохнуть или заняться любимым делом.\n"
        "Хотите узнать расписание уроков?"
    )
    tts = text
    return text, tts


def tell_about_homework(list_of_homework: List[Homework], tasks: int):

    text = f"Заданий: {tasks}"
    tts = "Всего " + __how_many_tasks(tasks)
    for hw in list_of_homework:
        tts += __tell_about_task(hw.lesson, hw.task)
    tts += "sil<[200]> Скажите Повтори, если хотите послушать еще раз"
    if tasks > 3:
        tts += "Скажите Вперед Назад для пролистывания списка"

    return text, tts


def __tell_about_task(lesson: str, task: str):
    return f"sil<[200]> {lesson} sil<[300]> {task}"


def __how_many_tasks(n: int) -> str:
    tasks = morph.parse("задание")[0].make_agree_with_number(n).word
    return str(n) + " " + tasks


# endregion

# region Настройки


def start_setting():
    text = (
        "Сейчас добавим одного ученика. Затем, можно будет добавить ещё.\n"
        "Как зовут ученика? Скажите Фамилию и Имя."
    )

    tts = text
    return text, tts


def FIO_needs():
    text = "Надо обязательно назвать Фамилию и Имя ученика. Назовите, пожалуйста."
    tts = text

    return text, tts


def duplicate_name():
    text = "Уже добавили ученика с таким именем.\n" "Выберите, пожалуйста, другое имя."

    tts = text
    return text, tts


def start_setting_fallback():
    text = "Простите, я вас не поняла. Скажите имя ученика, пожалуйста."

    tts = "<speaker audio='alice-sounds-human-kids-1.opus'>" + text
    return text, tts


def what_school(name):

    if name == "Алиса":
        start_phrase = "Ух ты! Какое совпадение!"
    elif name in [
        "Андрей",
        "Тимур",
        "Егор",
        "Артем",
        "Артём",
        "Вова",
        "Владимир",
        "Володя",
        "Максим",
        "Макс",
    ]:
        start_phrase = "Надо же! Одного из создателей навыка тоже так зовут!"
    else:
        start_phrase = "Хорошо."

    text = f"{start_phrase} В какой школе учится? Скажите номер школы."
    tts = text

    return text, tts


def what_school_fallback():
    text = (
        "Простите, я вас не поняла.\n"
        "Скажите номер школы, пожалуйста. Например, Лицей 110"
    )

    tts = "<speaker audio='alice-sounds-human-kids-1.opus'>" + text
    return text, tts


def what_classnumber():
    text = "В каком классе учится? Скажите номер класса. Например, 5 А."
    tts = "В каком классе учится? Скажите номер класса. Например, пятый - А."
    return text, tts


def what_classnumber_fallback():
    text = "Простите, я вас не поняла. В каком классе сейчас учится? Например, 5 А."

    tts = (
        "<speaker audio='alice-sounds-human-kids-1.opus'>"
        "Простите, я вас не поняла."
        "В каком классе сейчас учится? Например, пятый А."
    )
    return text, tts


def incorrect_classnumber():
    text = "Извините, я не поняла номер класса. Назовите, пожалуйста, число от 1 до 11"
    tts = (
        "Извините, я не поняла номер класса. "
        "Назовите, пожалуйста, число от одного до одиннадцати."
    )

    return text, tts


def what_classletter():
    text = "Какая буква у класса?"
    tts = text

    return text, tts


def what_classlatter_fallback():
    text = "Простите, я вас не поняла. Подскажите, какая буква у класса? Например, А."

    tts = (
        "<speaker audio='alice-sounds-human-kids-1.opus'>"
        "Простите, я вас не поняла."
        "Подскажите, какая буква у класса? Например, А."
    )
    return text, tts


def incorrect_classletter():
    text = "Извините, я не поняла букву класса. Назовите, пожалуйста, букву от А до Я"
    tts = text

    return text, tts


def confirm_settings(name: str, school: str, class_id: str):
    text = (
        "Почти готово. Давайте всё проверим.\n"
        f"{name} учится в {school} школе, в классе {class_id}." + "\n"
        "Всё верно?"
    )
    tts = text

    return text, tts


def one_more_student():
    text = "Здорово! Всё запомнила. Добавим еще одного ученика?"
    tts = text

    return text, tts


def discard_settings():
    text = (
        "Ох, ну надо же! Все мы допускаем ошибки.\n"
        "Давайте попробуем еще раз. Как зовут ученика?"
    )

    tts = text
    return text, tts


def reset_settings():
    text = "Очень жаль! Надеюсь вы ещё вернетесь."
    tts = text

    return text, tts


def confirm_reset():
    text = "Вы уверены, что хотите сбросить все настройки?"
    tts = text

    return text, tts


def reject_reset():
    text = (
        "Уф, а я уже подумала, что вы уходите.\n"
        "Что будем искать, расписание или домашнее задание?"
    )
    tts = text

    return text, tts


# endregion


def no_settings():
    text = "Пока что не выполнены базовые настройки.\n" + "Хотите сделать это сейчас?"
    tts = text

    return text, tts


# region Выбор ученика


def choose_homework(students: List[Student]):
    text = """Чье домашнее задание хотите узнать?"""
    last = students.pop()
    tts = (
        text
        + " ,".join([__inflect(s.name, {"gent"}).capitalize() for s in students])
        + " или "
        + __inflect(last.name, {"gent"}).capitalize()
    )

    return text, tts


def choose_schedule(students: List[Student]):
    text = """Чье расписание хотите узнать?"""
    last = students.pop()
    tts = (
        text
        + " ,".join([__inflect(s.name, {"gent"}).capitalize() for s in students])
        + " или "
        + __inflect(last.name, {"gent"}).capitalize()
    )

    return text, tts


def choose_student_fallback(students: List[Student]):
    text = "Извините, я вас не поняла.\n" + "Повторите, пожалуйста, имя ученика"
    last = students.pop()
    tts = (
        text
        + " ,".join([__inflect(s.name, {"gent"}).capitalize() for s in students])
        + " или "
        + __inflect(last.name, {"gent"}).capitalize()
    )

    return text, tts


def wrong_student_fallback(students: List[Student]):
    text = (
        "Не нашла настроек ученика с таким именем.\n"
        "Выберите, пожалуйста, имя ученика"
    )
    last = students.pop()
    tts = (
        text
        + " ,".join([__inflect(s.name, {"gent"}).capitalize() for s in students])
        + " или "
        + __inflect(last.name, {"gent"}).capitalize()
    )

    return text, tts


def not_found(students: list):
    list_of_students = " ,".join(students)

    text = (
        "Не нашла настроек ученика с таким именем.\n"
        "Сейчас вы можете проверить учеников:\n"
        f"{list_of_students}."
    )
    tts = text + "Повторите еще раз, пожалуйста"

    return text, tts


# endregion


def title(student, date):
    if date is None:
        str_date = "Сегодня"
    elif date.date() in KNOWN_DATES:
        str_date = KNOWN_DATES[date.date()]
    else:
        str_date = datetime.date.strftime(date.date(), "%d %B")

    text = student.name.capitalize() + ". " + str_date
    tts = f"У {__inflect(student.name, {'gent'})} на {str_date}"

    return text, tts


def _days_between(d1, d2):

    return abs((d2 - d1).days)


KNOWN_DATES = {
    datetime.date.today(): "Сегодня",
    datetime.date.today() + datetime.timedelta(days=1): "Завтра",
    datetime.date.today() + datetime.timedelta(days=2): "Послезавтра",
    datetime.date.today() + datetime.timedelta(days=-1): "Вчера",
    datetime.date.today() + datetime.timedelta(days=-2): "Позавчера",
}
