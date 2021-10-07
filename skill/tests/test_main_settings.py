import skill.main as main
import skill.state as state
from skill.tests.mock_alice import AliceAnswer, AliceEntity, AliceIntent, AliceRequest

# region start


def test_start_settings_confirm():

    req = (
        AliceRequest().from_scene("Welcome").add_intent(AliceIntent().confirm()).build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.next_scene == "Settings_FirstScene"


def test_start_settings_reject():

    req = (
        AliceRequest().from_scene("Welcome").add_intent(AliceIntent().reject()).build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.next_scene == "MaybeHelp"


def test_say_name():

    req = (
        AliceRequest()
        .from_scene("Settings_FirstScene")
        .add_entity(AliceEntity().fio(first_name="Добрыня"))
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_NAME) == "Добрыня"


def test_dont_say_name():

    req = (
        AliceRequest()
        .from_scene("Settings_FirstScene")
        .set_command("Слава Империи")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_NAME) is None
    assert answer.get_state_session(state.NEED_FALLBACK)
    assert answer.next_scene == "Settings_FirstScene"
    assert "Простите, я вас не поняла." in answer.text


def test_say_num_school():

    req = (
        AliceRequest()
        .from_scene("Settings_GetSchool")
        .add_entity(AliceEntity().number(777))
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)
    assert answer.get_state_session(state.TEMP_SCHOOL) == 777
    assert answer.next_scene == "Settings_GetClassNumber"


def test_dont_say_num_school():

    req = (
        AliceRequest()
        .from_scene("Settings_GetSchool")
        .set_command("Слава Империи")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_SCHOOL) is None
    assert answer.get_state_session(state.NEED_FALLBACK)
    assert answer.next_scene == "Settings_GetSchool"
    assert "Простите, я вас не поняла." in answer.text


def test_say_class_num_with_letter():

    req = (
        AliceRequest()
        .from_scene("Settings_GetClassNumber")
        .set_command("11 Б")
        .add_entity(AliceEntity().number(11))
        .add_to_state_session("temp_name", "name")
        .add_to_state_session("temp_last_name", "name")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)
    assert answer.get_state_session(state.TEMP_CLASS_ID) == "11Б"
    assert answer.next_scene == "Settings_Confirm"


def test_dont_say_class_num():

    req = (
        AliceRequest()
        .from_scene("Settings_GetClassNumber")
        .set_command("Слава Империи")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_CLASS_ID) is None
    assert answer.get_state_session(state.NEED_FALLBACK)
    assert answer.next_scene == "Settings_GetClassNumber"
    assert "Простите, я вас не поняла." in answer.text


def test_say_illegal_class_num():
    req = (
        AliceRequest()
        .from_scene("Settings_GetClassNumber")
        .set_command("100 Б")
        .add_entity(AliceEntity().number(100))
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_CLASS_ID) is None
    assert answer.next_scene == "Settings_IncorrectClassNumber"
    assert "Извините, я не поняла номер класса." in answer.text


def test_say_class_num_without_letter():

    req = (
        AliceRequest()
        .from_scene("Settings_GetClassNumber")
        .set_command("11")
        .add_entity(AliceEntity().number(11))
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)
    assert answer.get_state_session(state.TEMP_CLASS_ID) == "11"
    assert answer.next_scene == "Settings_GetClassLetter"


def test_say_class_letter():

    req = (
        AliceRequest()
        .from_scene("Settings_GetClassLetter")
        .set_command("Б")
        .add_to_state_session("temp_name", "name")
        .add_to_state_session("temp_last_name", "name")
        .add_to_state_session(state.TEMP_CLASS_ID, "11")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)
    assert answer.get_state_session(state.TEMP_CLASS_ID) == "11Б"
    assert answer.next_scene == "Settings_Confirm"


def test_dont_say_class_letter():

    req = (
        AliceRequest()
        .from_scene("Settings_GetClassLetter")
        .set_command("Слава Империи")
        .add_to_state_session(state.TEMP_CLASS_ID, "11")
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_CLASS_ID) == "11"
    assert answer.get_state_session(state.NEED_FALLBACK)
    assert answer.next_scene == "Settings_GetClassLetter"
    assert "Простите, я вас не поняла. Подскажите, какая буква у класса?" in answer.text


def test_say_illegal_class_letter():
    req = (
        AliceRequest()
        .from_scene("Settings_GetClassLetter")
        .set_command("Z")
        .add_to_state_session(state.TEMP_CLASS_ID, "11")
        .add_entity(AliceEntity().number(100))
        .build()
    )
    ans = main.handler(req, None)

    answer = AliceAnswer(ans)

    assert answer.get_state_session(state.TEMP_CLASS_ID) == "11"
    assert answer.next_scene == "Settings_IncorrectClassLetter"
    assert "Извините, я не поняла букву класса." in answer.text
