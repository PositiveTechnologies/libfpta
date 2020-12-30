/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
 *  Copyright 2016-2020 Leonid Yuriev <leo@yuriev.ru>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "details.h"

static int fpta_cursor_seek(fpta_cursor *cursor,
                            const MDBX_cursor_op mdbx_seek_op,
                            const MDBX_cursor_op mdbx_step_op,
                            const MDBX_val *mdbx_seek_key,
                            const MDBX_val *mdbx_seek_data);

int fpta_cursor_close(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);

  if (likely(rc == FPTA_SUCCESS) || rc == FPTA_TXN_CANCELLED) {
    mdbx_cursor_close(cursor->mdbx_cursor);
    fpta_cursor_free(cursor->db, cursor);
    rc = FPTA_SUCCESS;
  }

  return rc;
}

int fpta_cursor_open(fpta_txn *txn, fpta_name *column_id, fpta_value range_from,
                     fpta_value range_to, fpta_filter *filter,
                     fpta_cursor_options options, fpta_cursor **pcursor) {
  if (unlikely(pcursor == nullptr))
    return FPTA_EINVAL;
  *pcursor = nullptr;

  switch (options & ~(fpta_dont_fetch | fpta_zeroed_range_is_point)) {
  default:
    return FPTA_EFLAG;

  case fpta_descending:
  case fpta_unsorted:
  case fpta_ascending:
    break;
  }

  int rc = fpta_id_validate(column_id, fpta_column);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_name *table_id = column_id->column.table;
  rc = fpta_name_refresh_couple(txn, table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!fpta_is_indexed(column_id->shove)))
    return FPTA_NO_INDEX;

  if (unlikely(!fpta_index_is_compat(column_id->shove, range_from) ||
               !fpta_index_is_compat(column_id->shove, range_to)))
    return FPTA_ETYPE;

  if (unlikely(
          range_from.type == fpta_end || range_to.type == fpta_begin ||
          (range_from.type == fpta_epsilon && range_to.type == fpta_epsilon)))
    return FPTA_EINVAL;

  MDBX_dbi tbl_handle, idx_handle;
  rc = fpta_open_column(txn, column_id, tbl_handle, idx_handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fpta_index_type index = fpta_shove2index(column_id->shove);
  if (fpta_index_is_unordered(index) &&
      unlikely(fpta_cursor_is_ordered(options)))
    return FPTA_NO_INDEX;

  rc = fpta_name_refresh_filter(txn, column_id->column.table, filter);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!fpta_filter_validate(filter)))
    return FPTA_EINVAL;

  fpta_db *db = txn->db;
  fpta_cursor *cursor = fpta_cursor_alloc(db);
  if (unlikely(cursor == nullptr))
    return FPTA_ENOMEM;

  cursor->options = options & /* Сбрасываем флажок fpta_zeroed_range_is_point,
                                 чтобы в дальнейшем использовать его только как
                                 признак необходимости epsilon-обработки */
                    ~fpta_zeroed_range_is_point;
  cursor->txn = txn;
  cursor->table_id = table_id;
  cursor->column_number = column_id->column.num;
  cursor->tbl_handle = tbl_handle;
  cursor->idx_handle = idx_handle;

  assert(cursor->seek_range_flags == 0);
  if (range_from.type <= fpta_shoved) {
    rc = fpta_index_value2key(cursor->index_shove(), range_from,
                              cursor->range_from_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
    assert(cursor->range_from_key.mdbx.iov_base != nullptr);
    cursor->seek_range_flags |= fpta_cursor::need_cmp_range_from;
  }

  if (range_to.type <= fpta_shoved) {
    rc = fpta_index_value2key(cursor->index_shove(), range_to,
                              cursor->range_to_key, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout;
    assert(cursor->range_to_key.mdbx.iov_base != nullptr);
    cursor->seek_range_flags |= fpta_cursor::need_cmp_range_to;
  }

  rc =
      mdbx_cursor_open(txn->mdbx_txn, cursor->idx_handle, &cursor->mdbx_cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  if (range_from.type <= fpta_shoved && range_to.type <= fpta_shoved) {
    if (fpta_index_is_unordered(index) ||
        (options & fpta_zeroed_range_is_point) != 0) {
      const auto cmp =
          mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                   &cursor->range_from_key.mdbx, &cursor->range_to_key.mdbx);
      if (cmp == 0) {
        if ((options & fpta_zeroed_range_is_point))
          /* При наличии fpta_zeroed_range_is_point в исходных опциях и нулевом
             диапазоне - взводим флажок fpta_zeroed_range_is_point как признак
             необходимости epsilon-обработки. */
          cursor->options |= fpta_zeroed_range_is_point;
      } else if (unlikely(fpta_index_is_unordered(index))) {
        rc = FPTA_NO_INDEX;
        goto bailout;
      }
    }
  } else if (range_from.type == fpta_epsilon || range_to.type == fpta_epsilon) {
    if (range_from.type == fpta_epsilon)
      cursor->range_from_key.mdbx = cursor->range_to_key.mdbx;
    cursor->range_to_key.mdbx = cursor->range_from_key.mdbx;

    /* Взводим флажок fpta_zeroed_range_is_point
       как признак необходимости epsilon-обработки. */
    cursor->options |= fpta_zeroed_range_is_point;
    cursor->seek_range_flags = cursor->range_from_key.mdbx.iov_base
                                   ? fpta_cursor::need_cmp_range_both
                                   : fpta_cursor::need_key4epsilon;
    if ((options & fpta_dont_fetch) != 0 &&
        !cursor->range_from_key.mdbx.iov_base) {
      assert(cursor->range_to_key.mdbx.iov_base == nullptr);
      assert(range_from.type == fpta_epsilon || range_from.type == fpta_begin);
      assert(range_to.type == fpta_epsilon || range_to.type == fpta_end);
      /* Если была запрошена комбинация fpta_epsilon c fpta_begin/fpta_end,
       * вместе с опцией fpta_dont_fetch, то всё равно требуется выполнить
       * seek-операцию в начало или конец для защелкивания/переноса значения
       * ключа в границы диапазона. Причем ВАЖНО сделать это до установки
       * cursor->filter, чтобы не порождать неожиданных эффектов для
       * пользователя. */
      rc = fpta_cursor_seek(cursor,
                            ((range_from.type == fpta_begin) !=
                             fpta_cursor_is_descending(cursor->options))
                                ? MDBX_FIRST
                                : MDBX_LAST,
                            MDBX_cursor_op(-1 /* intentional invalid */),
                            nullptr, nullptr);
      if (unlikely(rc != MDBX_SUCCESS) && rc != FPTA_NODATA)
        goto bailout;
    }
  }

  cursor->filter = filter;
  if ((options & fpta_dont_fetch) == 0) {
    rc = fpta_cursor_move(cursor, fpta_first);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  *pcursor = cursor;
  return FPTA_SUCCESS;

bailout:
  if (cursor->mdbx_cursor)
    mdbx_cursor_close(cursor->mdbx_cursor);
  fpta_cursor_free(db, cursor);
  return rc;
}

//----------------------------------------------------------------------------

int fpta_cursor::bring(MDBX_val *key, MDBX_val *data, const MDBX_cursor_op op) {
  cxx11_constexpr_var unsigned ops_scan_mask =
      1 << MDBX_NEXT | 1 << MDBX_NEXT_DUP | 1 << MDBX_NEXT_MULTIPLE |
      1 << MDBX_NEXT_NODUP | 1 << MDBX_PREV | 1 << MDBX_PREV_DUP |
      1 << MDBX_PREV_NODUP | 1 << MDBX_PREV_MULTIPLE | 1 << MDBX_FIRST |
      1 << MDBX_FIRST_DUP | 1 << MDBX_LAST | 1 << MDBX_LAST_DUP;
  cxx11_constexpr_var unsigned ops_search_mask =
      1 << MDBX_GET_BOTH | 1 << MDBX_GET_BOTH_RANGE | 1 << MDBX_SET |
      1 << MDBX_SET_KEY | 1 << MDBX_SET_RANGE;

  metrics.scans += 1 & (ops_scan_mask >> op);
  metrics.searches += 1 & (ops_search_mask >> op);
  return mdbx_cursor_get(mdbx_cursor, key, data, op);
}

static inline bool is_forward_direction(MDBX_cursor_op op) {
  cxx11_constexpr_var unsigned mask =
      1 << MDBX_NEXT | 1 << MDBX_NEXT_DUP | 1 << MDBX_NEXT_MULTIPLE |
      1 << MDBX_NEXT_NODUP | 1 << MDBX_LAST | 1 << MDBX_LAST_DUP;
  return 1 & (mask >> op);
}

static inline bool is_backward_direction(MDBX_cursor_op op) {
  cxx11_constexpr_var unsigned mask =
      1 << MDBX_PREV | 1 << MDBX_PREV_DUP | 1 << MDBX_PREV_NODUP |
      1 << MDBX_PREV_MULTIPLE | 1 << MDBX_FIRST | 1 << MDBX_FIRST_DUP;
  return 1 & (mask >> op);
}

static int fpta_cursor_seek(fpta_cursor *cursor,
                            const MDBX_cursor_op mdbx_seek_op,
                            const MDBX_cursor_op mdbx_step_op,
                            const MDBX_val *mdbx_seek_key,
                            const MDBX_val *mdbx_seek_data) {
  assert(mdbx_seek_key != &cursor->current);
  int rc;
  fptu_ro mdbx_data;
  mdbx_data.sys.iov_base = nullptr;
  mdbx_data.sys.iov_len = 0;

  if (likely(mdbx_seek_key == NULL)) {
    assert(mdbx_seek_data == NULL);
    rc = cursor->bring(&cursor->current, &mdbx_data.sys, mdbx_seek_op);
  } else {
    /* Помещаем целевой ключ и данные (адреса и размеры)
     * в cursor->current и mdbx_data, это требуется для того чтобы:
     *   - после возврата из mdbx_cursor_get() в cursor->current и в mdbx_data
     *     уже были указатели на ключ и данные в БД, без необходимости
     *     еще одного вызова mdbx_cursor_get(MDBX_GET_CURRENT).
     *   - если передать непосредственно mdbx_seek_key и mdbx_seek_data,
     *     то исходные значения будут потеряны (перезаписаны), что создаст
     *     сложности при последующей корректировке позиции. Например, для
     *     перемещения за lower_bound для descending в fpta_cursor_locate().
     */
    cursor->current.iov_len = mdbx_seek_key->iov_len;
    cursor->current.iov_base =
        /* Замещаем nullptr для ключей нулевой длинны, так чтобы
         * в курсоре стоящем на строке с ключом нулевой длины
         * cursor->current.iov_base != nullptr, и тем самым курсор
         * не попадал под критерий is_poor() */
        mdbx_seek_key->iov_base ? mdbx_seek_key->iov_base : (void *)&fpta_NIL;

    if (!mdbx_seek_data) {
      rc = cursor->bring(&cursor->current, &mdbx_data.sys, mdbx_seek_op);
    } else {
      mdbx_data.sys = *mdbx_seek_data;
      rc = cursor->bring(&cursor->current, &mdbx_data.sys, mdbx_seek_op);
      if (likely(rc == MDBX_SUCCESS))
        rc = cursor->bring(&cursor->current, &mdbx_data.sys, MDBX_GET_CURRENT);
    }

    if (rc == MDBX_SUCCESS) {
      assert(cursor->current.iov_base != mdbx_seek_key->iov_base);
      if (mdbx_seek_data)
        assert(mdbx_data.sys.iov_base != mdbx_seek_data->iov_base);
    }

    if (fpta_cursor_is_descending(cursor->options) &&
        (mdbx_seek_op == MDBX_GET_BOTH_RANGE ||
         mdbx_seek_op == MDBX_SET_RANGE)) {
      /* Корректировка перемещения для курсора с сортировкой по-убыванию.
       *
       * Внутри mdbx_cursor_get() выполняет позиционирование аналогично
       * std::lower_bound() при сортировке по-возрастанию. Поэтому при
       * поиске для курсора с сортировкой в обратном порядке необходимо
       * выполнить махинации:
       *  - При MDBX_NOTFOUND ключ в самой последней строке (в порядке ключей)
       *    меньше искомого. Тогда следует перейти к последней строке, что будет
       *    соответствовать переходу к первой позиции при обратной сортировке.
       *  - Если ключ в позиции курсора больше искомого, то следует перейти к
       *    предыдущей строке, что будет соответствовать поведению lower_bound
       *    при сортировке в обратном порядке.
       *  - Если искомый ключ найден, то перейти к "первой" равной строке
       *    в порядке курсора, что означает перейти к последнему дубликату,
       *    либо к предыдущему дубликату.
       */
      if (rc == MDBX_SUCCESS) {
        const auto cmp = mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                                  &cursor->current, mdbx_seek_key);
        if (cmp > 0) {
          rc = cursor->bring(&cursor->current, &mdbx_data.sys, MDBX_PREV_NODUP);
          if (rc == MDBX_SUCCESS && mdbx_seek_op == MDBX_GET_BOTH_RANGE)
            rc = cursor->bring(&cursor->current, &mdbx_data.sys, MDBX_LAST_DUP);
        } else if (cmp == 0 && mdbx_seek_op == MDBX_GET_BOTH_RANGE &&
                   mdbx_dcmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                             &mdbx_data.sys, mdbx_seek_data) > 0) {
          rc = cursor->bring(&cursor->current, &mdbx_data.sys, MDBX_PREV);
        }
      } else if (rc == MDBX_NOTFOUND &&
                 mdbx_cursor_on_last(cursor->mdbx_cursor) == MDBX_RESULT_TRUE) {
        rc = cursor->bring(&cursor->current, &mdbx_data.sys, MDBX_LAST);
      }
    }
  }

  if (unlikely(cursor->seek_range_flags == fpta_cursor::need_key4epsilon)) {
    /* Если при установленном флажке fpta_zeroed_range_is_point не задана
       граница диапазона, то значит был передан псевдо-тип fpta_epsilon в
       комбинации c fpta_begin/fpta_end. Соответственно, требуется ограничить
       выборку строками со значением ключа равным первой или последней
       строке. Для этого достаточно перенести текущий ключ в границы
       диапазона при первой seek-операции в конец или начало. */
    assert(cursor->range_from_key.mdbx.iov_base == nullptr &&
           cursor->range_to_key.mdbx.iov_base == nullptr);
    assert(mdbx_seek_op == MDBX_FIRST || mdbx_seek_op == MDBX_LAST);
    assert(cursor->current.iov_len <= sizeof(cursor->range_from_key.place));
    cursor->range_from_key.mdbx.iov_len =
        std::min(cursor->current.iov_len,
                 /* paranoia */ sizeof(cursor->range_from_key.place));
    cursor->range_from_key.mdbx.iov_base =
        ::memcpy(&cursor->range_from_key.place, cursor->current.iov_base,
                 cursor->range_from_key.mdbx.iov_len);
    cursor->range_to_key.mdbx = cursor->range_from_key.mdbx;
    cursor->seek_range_state = cursor->seek_range_flags =
        fpta_cursor::need_cmp_range_both;
  }

  while (rc == MDBX_SUCCESS) {
    MDBX_cursor_op step_op = mdbx_step_op;

    if (cursor->seek_range_state & fpta_cursor::need_cmp_range_from) {
      const auto cmp = mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                                &cursor->current, &cursor->range_from_key.mdbx);
      if (cmp < 0) {
        /* задана нижняя граница диапазона и текущий ключ меньше её */
        switch (step_op) {
        default:
          assert(false);
        case MDBX_PREV_DUP:
        case MDBX_NEXT_DUP:
          /* нет смысла идти по дубликатам (без изменения значения ключа) */
          break;
        case MDBX_PREV:
        case MDBX_PREV_NODUP:
          /* нет смысла идти в сторону уменьшения ключа */
          break;
        case MDBX_NEXT:
          /* при движении в сторону увеличения ключа логично пропустить все
           * дубликаты, так как они заведомо не попадают в диапазон курсора */
          step_op = MDBX_NEXT_NODUP;
        // fall through
        case MDBX_NEXT_NODUP:
          goto next;
        }
        goto eof;
      } else if (is_forward_direction(step_op)) {
        /* больше не нужно сравнивать с range_from_key,
         * ибо остальные ключи буду больше или равны */
        cursor->seek_range_state -= fpta_cursor::need_cmp_range_from;
      }
    }

    if (cursor->seek_range_state & fpta_cursor::need_cmp_range_to) {
      const auto cmp = mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                                &cursor->current, &cursor->range_to_key.mdbx);
      if (cmp >=
          /* При установленном признаке fpta_zeroed_range_is_point включаем в
             выборку строки с равным ключом.
             Для этом проверяем условие cmp >= 1, что равноценно cmp > 0 */
          (unlikely(cursor->options & fpta_zeroed_range_is_point) ? 1 : 0)) {
        /* задана верхняя граница диапазона и текущий ключ больше её */
        switch (step_op) {
        default:
          assert(false);
        case MDBX_PREV_DUP:
        case MDBX_NEXT_DUP:
          /* нет смысла идти по дубликатам (без изменения значения ключа) */
          break;
        case MDBX_PREV:
          /* при движении в сторону уменьшения ключа логично пропустить все
           * дубликаты, так как они заведомо не попадают в диапазон курсора */
          step_op = MDBX_PREV_NODUP;
        // fall through
        case MDBX_PREV_NODUP:
          goto next;
        case MDBX_NEXT:
        case MDBX_NEXT_NODUP:
          /* нет смысла идти в сторону увеличения ключа */
          break;
        }
        goto eof;
      } else if (is_backward_direction(mdbx_step_op)) {
        /* больше не нужно сравнивать с range_to_key,
         * ибо остальные ключи буду меньше или равны */
        cursor->seek_range_state -= fpta_cursor::need_cmp_range_to;
      }
    }

    if (!cursor->filter) {
      cursor->metrics.results += 1;
      return FPTA_SUCCESS;
    }

    if (fpta_index_is_secondary(cursor->index_shove())) {
      MDBX_val pk_key = mdbx_data.sys;
      mdbx_data.sys.iov_base = nullptr;
      mdbx_data.sys.iov_len = 0;
      cursor->metrics.pk_lookups += 1;
      rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                    &mdbx_data.sys);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }

    if (fpta_filter_match(cursor->filter, mdbx_data)) {
      cursor->metrics.results += 1;
      return FPTA_SUCCESS;
    }

  next:
    rc = cursor->bring(&cursor->current, &mdbx_data.sys, step_op);
  }

  if (unlikely(rc != MDBX_NOTFOUND)) {
    cursor->set_poor();
    return rc;
  }

eof:
  switch (mdbx_seek_op) {
  default:
    cursor->set_poor();
    cursor->seek_range_state = 0;
    return FPTA_NODATA;

  case MDBX_NEXT:
  case MDBX_NEXT_NODUP:
    cursor->set_eof(fpta_cursor::after_last);
    cursor->seek_range_state = 0;
    return FPTA_NODATA;

  case MDBX_PREV:
  case MDBX_PREV_NODUP:
    cursor->set_eof(fpta_cursor::before_first);
    cursor->seek_range_state = 0;
    return FPTA_NODATA;

  case MDBX_PREV_DUP:
  case MDBX_NEXT_DUP:
    return FPTA_NODATA;
  }
}

int fpta_cursor_move(fpta_cursor *cursor, fpta_seek_operations op) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(op < fpta_first || op > fpta_key_prev)) {
    cursor->set_poor();
    return FPTA_EFLAG;
  }

  if (fpta_cursor_is_descending(cursor->options))
    op = fpta_seek_operations(op ^ 1);

  MDBX_val *mdbx_seek_key = nullptr;
  MDBX_cursor_op mdbx_seek_op, mdbx_step_op;
  switch (op) {
  default:
    assert(false && "unexpected seek-op");
    cursor->set_poor();
    return FPTA_EOOPS;

  case fpta_first:
    mdbx_seek_op = MDBX_FIRST;
    if (cursor->range_from_key.mdbx.iov_base) {
      mdbx_seek_key = &cursor->range_from_key.mdbx;
      mdbx_seek_op = MDBX_SET_RANGE;
    }
    mdbx_step_op = MDBX_NEXT;
    cursor->seek_range_state = cursor->seek_range_flags;
    break;

  case fpta_last:
    mdbx_seek_op = MDBX_LAST;
    if (cursor->range_to_key.mdbx.iov_base) {
      mdbx_seek_key = &cursor->range_to_key.mdbx;
      mdbx_seek_op = MDBX_SET_RANGE;
    }
    mdbx_step_op = MDBX_PREV;
    cursor->seek_range_state = cursor->seek_range_flags;
    break;

  case fpta_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = mdbx_step_op = MDBX_NEXT;
    if (unlikely(cursor->is_before_first())) {
      mdbx_seek_op = MDBX_FIRST;
      cursor->seek_range_state = cursor->seek_range_flags;
    }
    break;
  case fpta_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = mdbx_step_op = MDBX_PREV;
    if (unlikely(cursor->is_after_last())) {
      mdbx_seek_op = MDBX_LAST;
      cursor->seek_range_state = cursor->seek_range_flags;
    }
    break;

  /* Перемещение по дубликатам значения ключа, в случае если
   * соответствующий индекс был БЕЗ флага fpta_index_uniq */
  case fpta_dup_first:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDBX_FIRST_DUP;
    mdbx_step_op = MDBX_NEXT_DUP;
    break;

  case fpta_dup_last:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_SUCCESS;
    mdbx_seek_op = MDBX_LAST_DUP;
    mdbx_step_op = MDBX_PREV_DUP;
    break;

  case fpta_dup_next:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_NODATA;
    mdbx_seek_op = MDBX_NEXT_DUP;
    mdbx_step_op = MDBX_NEXT_DUP;
    break;

  case fpta_dup_prev:
    if (unlikely(!cursor->is_filled()))
      return cursor->unladed_state();
    if (unlikely(fpta_index_is_unique(cursor->index_shove())))
      return FPTA_NODATA;
    mdbx_seek_op = MDBX_PREV_DUP;
    mdbx_step_op = MDBX_PREV_DUP;
    break;

  case fpta_key_next:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = mdbx_step_op = MDBX_NEXT_NODUP;
    if (unlikely(cursor->is_before_first())) {
      mdbx_seek_op = MDBX_FIRST;
      cursor->seek_range_state = cursor->seek_range_flags;
    }
    break;

  case fpta_key_prev:
    if (unlikely(cursor->is_poor()))
      return FPTA_ECURSOR;
    mdbx_seek_op = mdbx_step_op = MDBX_PREV_NODUP;
    if (unlikely(cursor->is_after_last())) {
      mdbx_seek_op = MDBX_LAST;
      cursor->seek_range_state = cursor->seek_range_flags;
    }
    break;
  }

  return fpta_cursor_seek(cursor, mdbx_seek_op, mdbx_step_op, mdbx_seek_key,
                          nullptr);
}

int fpta_cursor_locate(fpta_cursor *cursor, bool exactly, const fpta_value *key,
                       const fptu_ro *row) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely((key != nullptr) == (row != nullptr))) {
    /* Должен быть выбран один из режимов поиска. */
    cursor->set_poor();
    return FPTA_EINVAL;
  }

  if (!fpta_cursor_is_ordered(cursor->options)) {
    if (FPTA_PROHIBIT_NEARBY4UNORDERED && !exactly) {
      /* Отвергаем неточный поиск для неупорядоченного курсора (и индекса). */
      cursor->set_poor();
      return FPTA_EFLAG;
    }
    /* Принудительно включаем точный поиск для курсора без сортировки. */
    exactly = true;
  }

  /* устанавливаем базовый режим поиска */
  MDBX_cursor_op mdbx_seek_op = exactly ? MDBX_SET_KEY : MDBX_SET_RANGE;
  const MDBX_val *mdbx_seek_data = nullptr;

  fpta_key seek_key, pk_key;
  if (key) {
    /* Поиск по значению проиндексированной колонки, конвертируем его в ключ
     * для поиска по индексу. Дополнительных данных для поиска нет. */
    rc = fpta_index_value2key(cursor->index_shove(), *key, seek_key, false);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
    /* базовый режим поиска уже выставлен. */
  } else {
    /* Поиск по "образу" строки, получаем из строки-кортежа значение
     * проиндексированной колонки в формате ключа для поиска по индексу. */
    rc = fpta_index_row2key(cursor->table_schema(), cursor->column_number, *row,
                            seek_key, false);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }

    if (fpta_index_is_secondary(cursor->index_shove())) {
      /* Курсор связан со вторичным индексом. Для уточнения поиска можем
       * использовать только значение PK. */
      if (fpta_index_is_unique(cursor->index_shove())) {
        /* Не используем PK если вторичный индекс обеспечивает уникальность.
         * Базовый режим поиска уже был выставлен. */
      } else {
        /* Извлекаем и используем значение PK только если связанный с
         * курсором индекс допускает дубликаты. */
        rc = fpta_index_row2key(cursor->table_schema(), 0, *row, pk_key, false);
        if (rc == FPTA_SUCCESS) {
          /* Используем уточняющее значение PK только если в строке-образце
           * есть соответствующая колонка. При этом игнорируем отсутствие
           * колонки (ошибку FPTA_COLUMN_MISSING). */
          mdbx_seek_data = &pk_key.mdbx;
          mdbx_seek_op = exactly ? MDBX_GET_BOTH : MDBX_GET_BOTH_RANGE;
        } else if (rc != FPTA_COLUMN_MISSING) {
          cursor->set_poor();
          return rc;
        } else {
          /* в строке нет колонки с PK, базовый режим поиска уже выставлен. */
        }
      }
    } else {
      /* Курсор связан с первичным индексом. Для уточнения поиска можем
       * использовать только данные (значение) всей строки. Однако,
       * делаем это ТОЛЬКО при неточном поиске по индексу с дубликатами,
       * так как только в этом случае это выглядит рациональным:
       *  - При точном поиске, отличие в значении любой колонки, включая
       *    её отсутствие, даст отрицательный результат.
       *    Соответственно, это породит кардинальные отличия от поведения
       *    в других лучаях. Например, когда используется вторичный индекс.
       *  - Фактически будет выполнятется не позиционирование курсора, а
       *    некая комплекстная операция "найти заданную строку таблицы",
       *    полезность которой сомнительна. */
      if (!exactly && !fpta_index_is_unique(cursor->index_shove())) {
        /* базовый режим поиска уже был выставлен, переключаем только
         * для нечеткого поиска среди дубликатов (как описано выше). */
        mdbx_seek_data = &row->sys;
        mdbx_seek_op = MDBX_GET_BOTH_RANGE;
      }
    }
  }

  rc = fpta_cursor_seek(cursor, mdbx_seek_op,
                        fpta_cursor_is_descending(cursor->options) ? MDBX_PREV
                                                                   : MDBX_NEXT,
                        &seek_key.mdbx, mdbx_seek_data);
  if (unlikely(rc != FPTA_SUCCESS)) {
    cursor->set_poor();
    return rc;
  }

  if (!fpta_cursor_is_descending(cursor->options))
    return FPTA_SUCCESS;

  /* Корректируем позицию при обратном порядке строк (fpta_descending) */
  while (!exactly) {
    /* При неточном поиске для курсора с обратной сортировкой нужно перейти
     * на другую сторону от lower_bound, т.е. идти в обратном порядке
     * до значения меньшего или равного целевому (с учетом фильтра). */
    int cmp = mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle,
                       &cursor->current, &seek_key.mdbx);

    if (cmp < 0)
      return FPTA_SUCCESS;

    if (cmp == 0) {
      if (!mdbx_seek_data) {
        /* Поиск без уточнения по дубликатам. Если индекс допускает
         * дубликаты, то следует перейти к последнему, что будет
         * сделано ниже. */
        break;
      }

      /* Неточный поиск с уточнением по дубликатам. Переход на другую
       * сторону lower_bound следует делать с учетом сравнения данных. */
      MDBX_val mdbx_data;
      rc = cursor->bring(&cursor->current, &mdbx_data, MDBX_GET_CURRENT);
      if (unlikely(rc != FPTA_SUCCESS)) {
        cursor->set_poor();
        return rc;
      }

      cmp = mdbx_dcmp(cursor->txn->mdbx_txn, cursor->idx_handle, &mdbx_data,
                      mdbx_seek_data);
      if (cmp <= 0)
        return FPTA_SUCCESS;
    }

    rc = fpta_cursor_seek(cursor, MDBX_PREV, MDBX_PREV, nullptr, nullptr);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
  }

  /* Для индекса с дубликатами нужно перейти к последней позиции с текущим
   * ключом. */
  if (!fpta_index_is_unique(cursor->index_shove())) {
    size_t dups;
    if (unlikely(mdbx_cursor_count(cursor->mdbx_cursor, &dups) !=
                 MDBX_SUCCESS)) {
      cursor->set_poor();
      return FPTA_EOOPS;
    }

    if (dups > 1) {
      /* Переходим к последнему дубликату (последнему мульти-значению
       * для одного значения ключа), а если значение не подходит под
       * фильтр, то двигаемся в обратном порядке дальше. */
      rc = fpta_cursor_seek(cursor, MDBX_LAST_DUP, MDBX_PREV, nullptr, nullptr);
      if (unlikely(rc != FPTA_SUCCESS)) {
        cursor->set_poor();
        return rc;
      }
    }
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_cursor_eof(const fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(cursor->is_filled()))
    return FPTA_SUCCESS;

  return FPTA_NODATA;
}

int fpta_cursor_state(const fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(cursor->is_filled()))
    return FPTA_SUCCESS;

  return cursor->unladed_state();
}

int fpta_cursor_count(fpta_cursor *cursor, size_t *pcount, size_t limit) {
  if (unlikely(!pcount))
    return FPTA_EINVAL;
  *pcount = (size_t)FPTA_DEADBEEF;

  size_t count = 0, metrics_results_before = cursor->metrics.results;
  int rc = fpta_cursor_move(cursor, fpta_first);
  while (rc == FPTA_SUCCESS && count < limit) {
    ++count;
    rc = fpta_cursor_move(cursor, fpta_next);
  }
  cursor->metrics.results = metrics_results_before + 1;

  if (rc == FPTA_SUCCESS || rc == FPTA_NODATA) {
    *pcount = count;
    rc = FPTA_SUCCESS;
  }

  cursor->set_poor();
  return rc;
}

int fpta_cursor_dups(fpta_cursor *cursor, size_t *pdups) {
  if (unlikely(pdups == nullptr))
    return FPTA_EINVAL;
  *pdups = (size_t)FPTA_DEADBEEF;

  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled())) {
    if (cursor->is_poor())
      return FPTA_ECURSOR;
    *pdups = 0;
    return FPTA_NODATA;
  }

  *pdups = 0;
  cursor->metrics.results += 1;
  rc = mdbx_cursor_count(cursor->mdbx_cursor, pdups);
  return (rc == MDBX_NOTFOUND) ? (int)FPTA_NODATA : rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_get(fpta_cursor *cursor, fptu_ro *row) {
  if (unlikely(row == nullptr))
    return FPTA_EINVAL;

  row->total_bytes = 0;
  row->units = nullptr;

  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  if (fpta_index_is_primary(cursor->index_shove()))
    return cursor->bring(&cursor->current, &row->sys, MDBX_GET_CURRENT);

  MDBX_val pk_key;
  rc = cursor->bring(&cursor->current, &pk_key, MDBX_GET_CURRENT);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  cursor->metrics.pk_lookups += 1;
  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key, &row->sys);
  return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
}

int fpta_cursor_key(fpta_cursor *cursor, fpta_value *key) {
  if (unlikely(key == nullptr))
    return FPTA_EINVAL;
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  rc = fpta_index_key2value(cursor->index_shove(), cursor->current, *key);
  return rc;
}

int fpta_cursor_delete(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  cursor->metrics.deletions += 1;
  if (!cursor->table_schema()->has_secondary()) {
    rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
    if (unlikely(rc != FPTA_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }
  } else {
    MDBX_val pk_key;
    if (fpta_index_is_primary(cursor->index_shove())) {
      pk_key = cursor->current;
      if (pk_key.iov_len > 0 &&
          /* LY: FIXME тут можно убрать вызов mdbx_is_dirty() и просто
           * всегда копировать ключ, так как это скорее всего дешевле. */
          mdbx_is_dirty(cursor->txn->mdbx_txn, pk_key.iov_base) !=
              MDBX_RESULT_FALSE) {
        void *buffer = alloca(pk_key.iov_len);
        pk_key.iov_base = memcpy(buffer, pk_key.iov_base, pk_key.iov_len);
      }
    } else {
      rc = cursor->bring(&cursor->current, &pk_key, MDBX_GET_CURRENT);
      if (unlikely(rc != MDBX_SUCCESS)) {
        cursor->set_poor();
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
      }
    }

    fptu_ro row;
#if defined(NDEBUG) && __cplusplus >= 201103L
    const cxx11_constexpr size_t likely_enough = 64u * 42u;
#else
    const size_t likely_enough = (time(nullptr) & 1) ? 11u : 64u * 42u;
#endif /* NDEBUG */
    void *buffer = alloca(likely_enough);
    row.sys.iov_base = buffer;
    row.sys.iov_len = likely_enough;

    cursor->metrics.upserts += 1;
    rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                      nullptr, &row.sys, MDBX_CURRENT);
    if (unlikely(rc == MDBX_RESULT_TRUE)) {
      assert(row.sys.iov_base == nullptr && row.sys.iov_len > likely_enough);
      row.sys.iov_base = alloca(row.sys.iov_len);
      cursor->metrics.upserts += 1;
      rc = mdbx_replace(cursor->txn->mdbx_txn, cursor->tbl_handle, &pk_key,
                        nullptr, &row.sys, MDBX_CURRENT);
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return rc;
    }

    rc = fpta_secondary_remove(cursor->txn, cursor->table_schema(), pk_key, row,
                               cursor->column_number);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    if (!fpta_index_is_primary(cursor->index_shove())) {
      rc = mdbx_cursor_del(cursor->mdbx_cursor, 0);
      if (unlikely(rc != MDBX_SUCCESS)) {
        cursor->set_poor();
        return fpta_internal_abort(cursor->txn, rc);
      }
    }
  }

  if (fpta_cursor_is_descending(cursor->options)) {
    /* Для курсора с обратным порядком строк требуется перейти к предыдущей
     * строке, в том числе подходящей под условие фильтрации. */
    fpta_cursor_seek(cursor, MDBX_PREV, MDBX_PREV, nullptr, nullptr);
  } else if (mdbx_cursor_eof(cursor->mdbx_cursor) == MDBX_RESULT_TRUE) {
    cursor->set_eof(fpta_cursor::after_last);
  } else {
    /* Для курсора с прямым порядком строк требуется перейти
     * к следующей строке подходящей под условие фильтрации, но
     * не выполнять переход если текущая строка уже подходит под фильтр. */
    fpta_cursor_seek(cursor, MDBX_GET_CURRENT, MDBX_NEXT, nullptr, nullptr);
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_cursor_validate_update_ex(fpta_cursor *cursor, fptu_ro new_row_value,
                                   fpta_put_options op) {
  if (unlikely(op != fpta_update &&
               op != (fpta_update | fpta_skip_nonnullable_check)))
    return FPTA_EFLAG;

  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  fpta_key column_key;
  rc = fpta_index_row2key(cursor->table_schema(), cursor->column_number,
                          new_row_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_KEY_MISMATCH;

  if ((op & fpta_skip_nonnullable_check) == 0) {
    rc = fpta_check_nonnullable(cursor->table_schema(), new_row_value);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  if (!cursor->table_schema()->has_secondary())
    return FPTA_SUCCESS;

  fptu_ro present_row;
  if (fpta_index_is_primary(cursor->index_shove())) {
    rc = cursor->bring(&cursor->current, &present_row.sys, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    cursor->metrics.uniq_checks += 1;
    return fpta_check_secondary_uniq(cursor->txn, cursor->table_schema(),
                                     present_row, new_row_value, 0);
  }

  MDBX_val present_pk_key;
  rc = cursor->bring(&cursor->current, &present_pk_key, MDBX_GET_CURRENT);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_schema(), 0, new_row_value, new_pk_key,
                          false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  cursor->metrics.pk_lookups += 1;
  rc = mdbx_get(cursor->txn->mdbx_txn, cursor->tbl_handle, &present_pk_key,
                &present_row.sys);
  if (unlikely(rc != MDBX_SUCCESS))
    return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;

  cursor->metrics.uniq_checks += 1;
  return fpta_check_secondary_uniq(cursor->txn, cursor->table_schema(),
                                   present_row, new_row_value,
                                   cursor->column_number);
}

int fpta_cursor_update(fpta_cursor *cursor, fptu_ro new_row_value) {
  int rc = fpta_cursor_validate(cursor, fpta_write);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  const fpta_table_schema *table_def = cursor->table_schema();
  rc = fpta_check_nonnullable(table_def, new_row_value);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_key column_key;
  rc = fpta_index_row2key(table_def, cursor->column_number, new_row_value,
                          column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!fpta_is_same(cursor->current, column_key.mdbx))
    return FPTA_KEY_MISMATCH;

  cursor->metrics.upserts += 1;
  if (!table_def->has_secondary()) {
    rc = mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                         &new_row_value.sys, MDBX_CURRENT | MDBX_NODUPDATA);
    if (likely(rc == MDBX_SUCCESS) &&
        /* актуализируем текущий ключ, если он был в грязной странице, то при
         * изменении мог быть перемещен с перезаписью старого значения */
        mdbx_is_dirty(cursor->txn->mdbx_txn, cursor->current.iov_base)) {
      rc = cursor->bring(&cursor->current, nullptr, MDBX_GET_CURRENT);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      cursor->set_poor();
    return rc;
  }

  MDBX_val old_pk_key;
  if (fpta_index_is_primary(cursor->index_shove())) {
    old_pk_key = cursor->current;
  } else {
    rc = cursor->bring(&cursor->current, &old_pk_key, MDBX_GET_CURRENT);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
    }
  }

  /* Здесь не очевидный момент при обновлении с изменением PK:
   *  - для обновления secondary индексов требуется как старое,
   *    так и новое значения строки, а также оба значения PK.
   *  - подготовленный old_pk_key содержит указатель на значение,
   *    которое физически размещается в value в служебной таблице
   *    secondary индекса, по которому открыт курсор.
   *  - если сначала, вызовом fpta_secondary_upsert(), обновить
   *    вспомогательные таблицы для secondary индексов, то указатель
   *    внутри old_pk_key может стать невалидным, т.е. так мы потеряем
   *    предыдущее значение PK.
   *  - если же сначала просто обновить строку в основной таблице,
   *    то будет утрачено её предыдущее значение, которое требуется
   *    для обновления secondary индексов.
   *
   * Поэтому, чтобы не потерять старое значение PK и одновременно избежать
   * лишних копирований, здесь используется mdbx_get_ex(). В свою очередь
   * mdbx_get_ex() использует MDBX_SET_KEY для получения как данных, так и
   * данных ключа. */

  fptu_ro old_row;
  cursor->metrics.pk_lookups += 1;
  rc = mdbx_get_ex(cursor->txn->mdbx_txn, cursor->tbl_handle, &old_pk_key,
                   &old_row.sys, nullptr);
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  fpta_key new_pk_key;
  rc = fpta_index_row2key(cursor->table_schema(), 0, new_row_value, new_pk_key,
                          false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

#if 0 /* LY: в данный момент нет необходимости */
  if (old_pk_key.iov_len > 0 &&
      mdbx_is_dirty(cursor->txn->mdbx_txn, old_pk_key.iov_base) !=
          MDBX_RESULT_FALSE) {
    void *buffer = alloca(old_pk_key.iov_len);
    old_pk_key.iov_base =
        memcpy(buffer, old_pk_key.iov_base, old_pk_key.iov_len);
  }
#endif

  rc = fpta_secondary_upsert(cursor->txn, cursor->table_schema(), old_pk_key,
                             old_row, new_pk_key.mdbx, new_row_value,
                             cursor->column_number);
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return fpta_internal_abort(cursor->txn, rc);
  }

  const bool pk_changed = !fpta_is_same(old_pk_key, new_pk_key.mdbx);
  if (pk_changed) {
    cursor->metrics.deletions += 1;
    rc = mdbx_del(cursor->txn->mdbx_txn, cursor->tbl_handle, &old_pk_key,
                  nullptr);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->tbl_handle, &new_pk_key.mdbx,
                  &new_row_value.sys, MDBX_NODUPDATA | MDBX_NOOVERWRITE);
    if (unlikely(rc != MDBX_SUCCESS)) {
      cursor->set_poor();
      return fpta_internal_abort(cursor->txn, rc);
    }

    rc = mdbx_cursor_put(cursor->mdbx_cursor, &column_key.mdbx,
                         &new_pk_key.mdbx, MDBX_CURRENT | MDBX_NODUPDATA);

  } else {
    rc = mdbx_put(cursor->txn->mdbx_txn, cursor->tbl_handle, &new_pk_key.mdbx,
                  &new_row_value.sys, MDBX_CURRENT | MDBX_NODUPDATA);
  }

  if (likely(rc == MDBX_SUCCESS) &&
      /* актуализируем текущий ключ, если он был в грязной странице, то при
       * изменении мог быть перемещен с перезаписью старого значения */
      mdbx_is_dirty(cursor->txn->mdbx_txn, cursor->current.iov_base)) {
    rc = cursor->bring(&cursor->current, nullptr, MDBX_GET_CURRENT);
  }
  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return fpta_internal_abort(cursor->txn, rc);
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_apply_visitor(
    fpta_txn *txn, fpta_name *column_id, fpta_value range_from,
    fpta_value range_to, fpta_filter *filter, fpta_cursor_options op,
    size_t skip, size_t limit, fpta_value *page_top, fpta_value *page_bottom,
    size_t *count, int (*visitor)(const fptu_ro *row, void *context, void *arg),
    void *visitor_context, void *visitor_arg) {

  if (unlikely(limit < 1 || !visitor))
    return FPTA_EINVAL;

  fpta_cursor *cursor = nullptr /* TODO: заменить на объект на стеке */;
  int rc =
      fpta_cursor_open(txn, column_id, range_from, range_to, filter,
                       (fpta_cursor_options)(op & ~fpta_dont_fetch), &cursor);

  for (; skip > 0 && likely(rc == FPTA_SUCCESS); --skip)
    rc = fpta_cursor_move(cursor, fpta_next);

  if (page_top) {
    if (rc == FPTA_SUCCESS) {
      int err = fpta_index_key2value(cursor->index_shove(), cursor->current,
                                     *page_top);
      assert(err == FPTA_SUCCESS);
      if (unlikely(err != FPTA_SUCCESS))
        rc = err;
    } else {
      *page_top = (rc == FPTA_NODATA) ? fpta_value_begin() : fpta_value_null();
    }
  }

  size_t n;
  for (n = 0; likely(rc == FPTA_SUCCESS) && n < limit; n++) {
    fptu_ro row;
    rc = fpta_cursor_get(cursor, &row);
    if (unlikely(rc != FPTA_SUCCESS))
      break;
    rc = visitor(&row, visitor_context, visitor_arg);
    if (unlikely(rc != FPTA_SUCCESS))
      break;
    rc = fpta_cursor_move(cursor, fpta_next);
  }

  if (count)
    *count = n;

  if (page_bottom) {
    if (cursor && cursor->is_filled()) {
      int err = fpta_index_key2value(cursor->index_shove(), cursor->current,
                                     *page_bottom);
      assert(err == FPTA_SUCCESS);
      if (unlikely(err != FPTA_SUCCESS))
        rc = err;
    } else {
      *page_bottom = (rc == FPTA_NODATA) ? fpta_value_end() : fpta_value_null();
    }
  }

  if (cursor) {
    int err = fpta_cursor_close(cursor);
    assert(err == FPTA_SUCCESS);
    if (unlikely(err != FPTA_SUCCESS))
      rc = err;
  }
  return rc;
}

//----------------------------------------------------------------------------

int fpta_cursor_info(fpta_cursor *cursor, fpta_cursor_stat *stat) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(stat == nullptr))
    return FPTA_EINVAL;

  stat->results = cursor->metrics.results;
  stat->index_searches = cursor->metrics.searches;
  stat->index_scans = cursor->metrics.scans;
  stat->pk_lookups = cursor->metrics.pk_lookups;
  stat->uniq_checks = cursor->metrics.uniq_checks;
  stat->upserts = cursor->metrics.upserts;
  stat->deletions = cursor->metrics.deletions;

  stat->selectivity_x1024 =
      (stat->results + stat->upserts + stat->deletions + 1) * 1024u /
      (stat->index_scans + stat->index_searches + stat->pk_lookups + 1);

  return FPTA_SUCCESS;
}

int fpta_cursor_reset_accounting(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  memset(&cursor->metrics, 0, sizeof(cursor->metrics));
  return FPTA_SUCCESS;
}

int fpta_cursor_rerere(fpta_cursor *cursor) {
  int rc = fpta_cursor_validate(cursor, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (cursor->txn->level > fpta_read)
    /* ничего не делаем для пишущих транзакций */
    return FPTA_SUCCESS;

#ifndef NDEBUG
  MDBX_txn_info info;
  rc = mdbx_txn_info(cursor->txn->mdbx_txn, &info, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (info.txn_reader_lag == 0)
    return FPTA_EINVAL;
#endif /* !NDEBUG */

  MDBX_val save_key, save_data;
  /* запоминаем позицию только если курсор установлен */
  if (likely(cursor->is_filled())) {
    rc = mdbx_cursor_get(cursor->mdbx_cursor, &save_key, &save_data,
                         MDBX_GET_CURRENT);
    if (likely(rc == FPTA_SUCCESS)) {
      save_key.iov_base = save_key.iov_len
                              ? memcpy(alloca(save_key.iov_len),
                                       save_key.iov_base, save_key.iov_len)
                              : nullptr;

      if (!fpta_index_is_unique(cursor->index_shove()))
        save_data.iov_base = save_data.iov_len
                                 ? memcpy(alloca(save_data.iov_len),
                                          save_data.iov_base, save_data.iov_len)
                                 : nullptr;
    }
  }

  /* всегда перезапускаем транзакцию и собираем ошибки */
  int err = fpta_transaction_restart(cursor->txn);
  rc = (err == MDBX_SUCCESS) ? rc : err;

  /* всегда обновляем курсор и собираем ошибки */
  err = mdbx_cursor_renew(cursor->txn->mdbx_txn, cursor->mdbx_cursor);
  rc = (err == MDBX_SUCCESS) ? rc : err;

  if (unlikely(rc != MDBX_SUCCESS)) {
    cursor->set_poor();
    return rc;
  }

  if (unlikely(!cursor->is_filled()))
    return cursor->unladed_state();

  const MDBX_cursor_op step_op =
      fpta_cursor_is_descending(cursor->options) ? MDBX_PREV : MDBX_NEXT;
  MDBX_cursor_op seek_op = MDBX_SET_RANGE;
  MDBX_val *seek_data = nullptr;
  if (!fpta_index_is_unique(cursor->index_shove())) {
    /* Для индекса без уникальности сначала нужно проверить что есть
     * сохраненный ключ. Если ключ есть, то продолжить искать ближайшую строку
     * к сохраненным данным. */
    rc = fpta_cursor_seek(cursor, seek_op, step_op, &save_key, nullptr);
    if (rc != FPTA_SUCCESS ||
        mdbx_cmp(cursor->txn->mdbx_txn, cursor->idx_handle, &cursor->current,
                 &save_key) != 0)
      return rc;

    seek_op = MDBX_GET_BOTH_RANGE;
    seek_data = &save_data;
  }

  return fpta_cursor_seek(cursor, seek_op, step_op, &save_key, seek_data);
}
