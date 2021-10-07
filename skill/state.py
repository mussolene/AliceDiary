STATE_REQUEST_KEY = "session"
STATE_RESPONSE_KEY = "session_state"
USERSTATE_RESPONSE_KEY = "user_state_update"

# Отладочная информация
# Последние фразы пользователя
PREVIOUS_MOVES = "prev_moves"

# признак уточнения после того, как не смогли разобрать фразу.
# Если снова не разобрали - выходим
NEED_FALLBACK = "fallback"

# region State of dialog

STUDENTS = "students"

# endregion

# help menu
PREVIOUS_STATE = "previous_state"
NEXT_BUTTON = "next_button"

# temporary
TEMP_SCHOOL = "temp_school"
TEMP_NAME = "temp_name"
TEMP_LAST_NAME = "temp_last_name"
TEMP_CLASS_ID = "temp_class_id"

# homework
LIST_HW = "list_hw"
SKIP_HW = "skip_hw"
TASKS_HW = "tasks_hw"

# Временный контекст для выбора ученика
TEMP_CONTEXT = "context"

# Эти состояния будут сохранены в fallback
MUST_BE_SAVE = {PREVIOUS_STATE, NEXT_BUTTON}

# Эти состояния сохраняются на каждый ход
PERMANENT_VALUES = {
    TEMP_NAME,
    TEMP_LAST_NAME,
    TEMP_SCHOOL,
    TEMP_CLASS_ID,
    LIST_HW,
    SKIP_HW,
    TASKS_HW,
}
