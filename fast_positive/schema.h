/*
 * Copyright 2016-2017 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
 *
 * The kind of lightweight linearized tuples, which are extremely handy
 * to machining, including cases with shared memory.
 * Please see README.md at https://github.com/leo-yuriev/libfptu
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * "Позитивные Кортежи" дают легковесное линейное представление небольших
 * JSON-подобных структур в экстремально удобной для машины форме,
 * в том числе при размещении в разделяемой памяти.
 */

#pragma once

#include "fast_positive/tuples.h"

/* ОБЩИЕ СВЕДЕНИЯ
 *
 * Если рассматривать схему упрощенно, то она содержит информацию необходимую
 * для конвертации кортежей в JSON-документы и обратно. В том числе, схему
 * можно рассматривать как словарь, в котором перечислены все соответствия
 * между именами JSON-полей и тегами fptu-полей с указанием их типа.
 * Соответствующий схеме документ не должен содержать неизвестных полей или
 * нарушать допустимый набор отношений, включая количественные ограничения.
 *
 * На самом деле fptu-схема выполняет несколько задач:
 *  1) Описывает иерархию типов, включая структуры. При этом в качестве
 *     корневых типов всегда используются нативные типы fptu.
 *  2) Описывает структуру записей-документов в виде дерева типизированных
 *     элементов/атрибутов с допустимым набором отношений/связей между
 *     ними (вхождение полей в структуры).
 *  3) Предлагает простой, но достаточный и удобный язык описания схемы,
 *     который обеспечивает развитие (эволюцию) схемы данных с контролируемой
 *     совместимостью с предыдущими версиями.
 *  4) Автоматизирует построение проекций для представления древовидных
 *     структур в виде плоских/одномерных кортежей, в том числе для
 *     хранения иерархических структур в колоночных базах данных.
 *  5) Предлагает машинно-эффективный справочник схемы.
 *
 * Следует отметить, что поддержка представления в виде fptu-кортежей и
 * поддержка плоских проекций подразумевает активное участие компилятора схемы,
 * в том числе автоматическое внесение назначенных идентификаторов в исходный
 * текст описания схемы, одновременно с генерацией справочника как машинного
 * представления схемы.
 *
 * Также немаловажно, что функционально схема специфицирует только набор
 * требований, ограничений и координаты полей для двух методов адресации:
 *   1) по именам полей (например в JSON или СУБД)
 *   2) по номерам/тэгам полей (например в fptu-кортежах).
 *
 * Этой информации достаточно для того, чтобы проверить соответствие некоторого
 * объекта/документа/записи схеме и/или получить координаты каждого описанного
 * в схеме элемента. Поэтому:
 *  - С одной стороны, схема достаточно аскетична, НЕ содержит лишней
 *    информации, легка и предельно эффективно в реализации.
 *  - С другой стороны, минимализм функционала вместе с выразительным языком
 *    описания, даёт несколько неожиданных "волшебных" (непривычных) свойств
 *    и позволяет легко манипулировать ими.
 *
 * В частности, схема обеспечивает эволюционное развитие, в том числе
 * с сохранением совместимости без необходимости пересборки приложений.
 */

/* ПРЕОБРАЗОВАНИЕ В ПЛОСКУЮ ФОРМУ
 *
 * Под "плоской" формой подразумевается проекция иерархической/древовидной
 * структуры (ограниченной вложенности и сложности) на одномерный набор полей.
 * Такая проекция позволяет хранить исходную структуру, как в плоском одном
 * кортеже без вложенности, так и в записи колоночной или реляционной СУБД.
 *
 * Кроме этого, в сравнении с иерархической/древовидной формой, такое плоское
 * представление, при соблюдении некоторых разумных ограничений, позволяет
 * существенно (нередко многократно) сократить накладные затраты при машинной
 * обработке.
 *
 * Подобное отображение возможно только при введении явных ограничений на
 * глубину вложенности структур и одновременно на количество экземпляров всех
 * вложенных полей, которые являются структурами.
 *
 * Для получения плоской проекции ограниченной древовидной структуры в виде
 * fptu-кортежа, необходимо и достаточно, каждому возможному полю в каждой
 * вложенной структуре, назначить свой уникальный тэг/номер. Проще говоря,
 * нам необходимо построчно выписать на листок максимально полный вариант
 * структуры, рекурсивно обходя все вложенные структуры и поля, а после
 * пронумеровать выписанные строки. Так мы назначим уникальный идентификатор
 * каждому допустимому экземпляру поля в каждом вложенном экземпляре структуры.
 *
 * Компилятор схемы автоматизирует и контролирует этот процесс, совмещая его
 * с назначением внутренних идентификаторов fptu-полей и построением
 * справочника схемы.
 */

/* ОСНОВНОЕ
 *
 * Для описания схемы поддерживается иерархическая модель типов с одинарным
 * наследованием (от множественного наследования решено отказаться как за
 * ненадобностью, так и ради упрощения). Иерархия наследования всегда начинается
 * с одного из нативных типов fptu, за вычетом массивов и вложенных кортежей.
 *
 * Базовым типом для структур является void, если явно не задано другого.
 * Возможность специфицировать для структур базовый тип может показаться
 * странной. Однако, на самом деле это но является ключевой возможностью для
 * эволюции схемы. Так например, в первой версии схемы некоторое поле может
 * иметь простой скалярный тип, а в следующей версии такое поле может быть
 * безболезненно расширено дополнительными атрибутами.
 *
 * Схему можно представить в виде двух ориентированных графов:
 *   - граф наследования: его узлами являются идентификаторы типов, а ребра
 *     определяют иерархию наследования. У этого графа несколько корней,
 *     каждый из которых является базовым типом fptu.
 *   - граф структуры: узлами которого являются идентификаторы полей, а ребра
 *     определяют вхождение поля в структуру, в том числе вложенность структур.
 *
 */

/* ИДЕНТИФИКАТОРЫ ПОЛЕЙ И ЭВОЛЮЦИЯ СХЕМЫ:
 *
 *
 */

#ifdef __cplusplus
//#include <limits> // for numeric_limits<>
//#include <string> // for std::string
extern "C" {
#endif

typedef uint16_t fptu_typeId;
typedef uint64_t fptu_typeKey;
typedef struct fptu_typeinfo fptu_typeinfo;
typedef struct fptu_schema_dict fptu_schema_dict;

struct fptu_typeinfo {
  fptu_typeId id_self;
  fptu_typeId id_parent;
  uint8_t /* fptu_type */ basetype;
  uint8_t flags;
  uint16_t elements_count;
  const char *name_full;
  const char *name_field;
};

struct fptu_schema_dict {
  void *internals;
  int (*method_typeinfo_by_id)(const fptu_schema_dict *, const fptu_typeId id,
                               fptu_typeinfo *result);
  int (*method_typeinfo_by_key)(const fptu_schema_dict *,
                                const fptu_typeKey key, fptu_typeinfo *result);
  int (*method_typeinfo_elements)(const fptu_schema_dict *,
                                  const fptu_typeinfo *, fptu_typeId *buffer,
                                  size_t buffer_length);

  int (*method_typeinfo_heirdom)(const fptu_schema_dict *,
                                 const fptu_typeId ancestor,
                                 const fptu_typeId successor);

  int (*method_typeinfo_nestable)(const fptu_schema_dict *,
                                  const fptu_typeId scope,
                                  const fptu_typeId field);
};

static __inline int fptu_typeinfo_by_id(const fptu_schema_dict *dict,
                                        const fptu_typeId id,
                                        fptu_typeinfo *result) {
  return dict->method_typeinfo_by_id(dict, id, result);
}

static __inline int fptu_typeinfo_by_key(const fptu_schema_dict *dict,
                                         const fptu_typeKey key,
                                         fptu_typeinfo *result) {
  return dict->method_typeinfo_by_key(dict, key, result);
}

static __inline int fptu_typeinfo_elements(const fptu_schema_dict *dict,
                                           const fptu_typeinfo *base,
                                           fptu_typeId *buffer,
                                           size_t buffer_length) {
  return dict->method_typeinfo_elements(dict, base, buffer, buffer_length);
}

int fptu_typeinfo_by_name(const fptu_schema_dict *, const char *name,
                          fptu_typeinfo *result);
int fptu_typeinfo_by_field(const fptu_schema_dict *, const fptu_typeinfo *base,
                           const char *field, fptu_typeinfo *result);

fptu_typeKey fptu_typekey(const char *name, size_t length);

#ifdef __cplusplus
}

namespace fptu {
namespace Schema {

inline fptu_typeId MakeTypeId(long ditto) { return (fptu_typeId)ditto; }

} /* namespace Schema */
} /* namespace fptu */

#endif /* __cplusplus */
