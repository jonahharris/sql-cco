#!/usr/bin/env node --expose-gc --max-old-space-size=8192
/* ========================================================================= **
**                     ________________  _____ ____    __                    **
**                    / ____/ ____/ __ \/ ___// __ \  / /                    **
**                   / /   / /   / / / /\__ \/ / / / / /                     **
**                  / /___/ /___/ /_/ /___/ / /_/ / / /___                   **
**                  \____/\____/\____//____/\___\_\/_____/                   **
**                                                                           **
** ========================================================================= **
**                   RECOMMENDER-OPTIMIZED INVERTED INDEX                    **
** ========================================================================= **
** SQL-Based Cross-cooccurrence Recommender System                           **
** Copyright (C) Jonah H. Harris <jonah.harris@gmail.com>                    **
** All Rights Reserved.                                                      **
**                                                                           **
** Permission to use, copy, modify, and/or distribute this software for any  **
** purpose is subject to the terms specified in the License Agreement.       **
** ========================================================================= */

const Heap = require('mnemonist/heap');
const fs = require('fs');
const microtime = require('microtime');
const path = require('path');
const zlib = require('zlib');

class CCOIndex {
  constructor (options = {}) {

    /* ===================================================================== */
    /* -- PUBLIC PROPERTIES ------------------------------------------------ */
    /* ===================================================================== */

    /** Self reference for lambda usage. */
    const self = this;

    /* ===================================================================== */
    /* -- PRIVATE PROPERTIES ----------------------------------------------- */
    /* ===================================================================== */

    /** Pre-optimized indexed documents stored by field. */
    this._isOptimized = false;

    /** Pre-optimized indexed documents stored by field. */
    this._fields = {};

    /** Interned string mapping used for (string:id lookup). */
    this._dictionary = {};

    /** Interned string used for (id:string lookup). */
    this._strings = [];

    /** The maximum id used for string internment. */
    this._id = 0;

    /** The optimized document index. */
    this._index = {};

    /** Controls non-linear term frequency normalization (saturation). */
    this._k1 = 1.2;

    /** Controls to what degree document length normalizes tf values. */
    this._b = 0.75;

    /** Whether to intern text to reduce space. */
    //this._shouldIntern = false;
    this._shouldIntern = true;

    /** Controls to what degree document length normalizes tf values. */
    this._scoreDigits = 4;

  } /* CCOIndex::constructor() */

  /* ======================================================================= */
  /* -- PUBLIC METHODS ----------------------------------------------------- */
  /* ======================================================================= */

  /**
   * Loads an optimized index in from a file.
   */
  async load (fileName) {
    let obj = null;
    if (fileName.includes('.gz')) {
      obj = JSON.parse(zlib.gunzipSync(await fs.promises.readFile(fileName)));
    } else {
      obj = JSON.parse(await fs.promises.readFile(fileName));
    }

    this._isOptimized = true;
    this._strings = obj.d;
    this._index = obj.t;

    for (let [ idx, str ] of Object.entries(this._strings)) {
      this._dictionary[`${str}`] = idx;
    }
  } /* CCOIndex::load() */

  /* ----------------------------------------------------------------------- */

  /**
   * Adds a document to an unoptimized index.
   */
  addDocument (doc) {
    if (this._isOptimized) {
      throw new Error('Cannot add a document to an already-optimized index.');
    }

    if (typeof doc.id === 'undefined') {
      throw new Error(1000, 'ID is a required property of documents.');
    };

    const fieldsToIndex = [];
    for (let [ fieldName, fieldData ] of Object.entries(doc)) {
      /* The document ID is not a field... */
      if ('id' === fieldName) {
        continue;
      }

      /* Make sure this field should be indexed... */
      if (!(Object.hasOwnProperty.call(this._fields, this._intern(fieldName, false)))) {
        console.error(`Document field (${fieldName}) is not indexable.`);
        continue;
      }

      fieldsToIndex.push(fieldName);
    }

    if (0 === fieldsToIndex.length) {
      console.log(`Not indexing empty document ${doc.id}`);
      return;
    }

    doc.id = this._intern(doc.id);

    for (let [ _, fieldName ] of Object.entries(fieldsToIndex)) {
      let tokens = this._tokenize(doc[`${fieldName}`], true);

      fieldName = this._intern(fieldName, false);

      if (!(Object.hasOwnProperty.call(this._fields[`${fieldName}`].documents, doc.id))) {
        this._fields[`${fieldName}`].documents[`${doc.id}`] = tokens.length;
      }

      for (let [ _, term ] of Object.entries(tokens)) {
        if (!(Object.hasOwnProperty.call(this._fields[`${fieldName}`].terms, term))) {
          this._fields[`${fieldName}`].terms[`${term}`] = {};
        }

        if (!(Object.hasOwnProperty.call(this._fields[`${fieldName}`].terms[`${term}`], doc.id))) {
          this._fields[`${fieldName}`].terms[`${term}`][`${doc.id}`] = 0;
        }
        ++this._fields[`${fieldName}`].terms[`${term}`][`${doc.id}`];
      }
    }
  } /* CCOIndex::addDocument() */

  /* ----------------------------------------------------------------------- */

  /**
   * Adds a field to an unoptimized index.
   */
  addField (fieldName) {
    if (this._isOptimized) {
      throw new Error('Cannot add a field to an already-optimized index.');
    }

    fieldName = this._intern(fieldName);
    this._fields[`${fieldName}`] = {
      terms: {},
      documents: {}
    };

    this._index[`${fieldName}`] = {};
  } /* CCOIndex::addField() */

  /* ----------------------------------------------------------------------- */

  /**
   * Override Object.prototyp.toSJSON to return only the optimized index.
   */
  toJSON () {
    if (this._isOptimized) {
      return {
        d: this._strings,
        t: this._index
      };
    } else {
      return {
        d: [],
        t: {}
      };
    }
  } /* CCOIndex::toJSON() */

  /* ----------------------------------------------------------------------- */

  /**
   * Override Object.prototyp.toString.
   */
  toString () {
    return JSON.stringify(this.toJSON());
  } /* CCOIndex::toString() */

  /* ----------------------------------------------------------------------- */

  /**
   *
   */
  optimize () {

    /*
     * We could throw an error, but the user wants an optimized index and we
     * already have one, so...
     */
    if (this._isOptimized) {
      return;
    }

    for (let [ fieldName, fieldData ] of Object.entries(this._fields)) {
      //console.log(`FIELD: ${fieldName}`);

      /* N, total number of documents with field */
      let N = Object.keys(this._fields[`${fieldName}`].documents).length;

      /* avgdl, average length of field */
      let dlsum = 0;
      let dlcount = 0;
      for (let [ _, tokenCount ] of Object.entries(fieldData.documents)) {
        dlsum += tokenCount;
        ++dlcount;
      }
      let avgdl = (dlsum / dlcount);

      /* calculate term frequencies and idf */
      for (let [ termName, termDocs ] of Object.entries(fieldData.terms)) {
        //console.log(`TERM: ${termName}`);

        /* n, number of documents containing term (in this field) */
        let n = Object.keys(this._fields[`${fieldName}`].terms[`${termName}`]).length;
        //console.log(`n, ${n}`);

        /* N, total number of documents with field */
        //console.log(`N, ${N}`);

        /* idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from: */
        let idf = Math.log(1 + (N - n + 0.5) / (n + 0.5));
        //console.log(`idf - ${idf}`);

        for (let [ docId, freq ] of Object.entries(termDocs)) {
          //console.log(`DOC ${docId}`);

          /* freq, occurrences of term within document */
          //fields[field].terms[term][documentId]
          //let freq = this._fields[`${fieldName}`].terms[`${termName}`][`${docId}`];
          //console.log(`${docId} freq, ${freq}`);

          /* k1, term saturation parameter */
          /* b, length normalization parameter */
          /* dl, length of field */
          let dl = this._fields[`${fieldName}`].documents[`${docId}`];
          //console.log(`${docId} dl, ${dl}`);

          /* avgdl, average length of field */
          //console.log(`${docId} avgdl, ${avgdl}`);

          /* tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from: */
          //let tf = freq / (freq + this._k1 * (1 - this._b + this._b * dl / avgdl));
          let tf = (freq / (freq + (this._k1 * (1 - this._b + (this._b * dl / avgdl)))));
          //console.log(`${docId} tf, ${tf}`);

          /* boost */
          let boost = 2.2;
          //console.log(`${docId} boost, ${boost}`);

          /* score(freq=1.0), computed as boost * idf * tf from: */
          let score = +(boost * idf * tf).toFixed(this._scoreDigits);
          //console.log(`${fieldName} ${docId} score, ${score}`);

          /* Overwrite */
          this._fields[`${fieldName}`].terms[`${termName}`][`${docId}`] = score;

          /* weight(message:elasticsearch in 0) [PerFieldSimilarity], result of: */
        }
      }
    }

    for (let [ fieldName, fieldData ] of Object.entries(this._fields)) {
      this._index[`${fieldName}`] = fieldData.terms;
    }

    delete this._fields;
    this._isOptimized = true;

  } /* CCOIndex::optimize() */

  /* ----------------------------------------------------------------------- */

  /**
   * Saves an optimized index to a file.
   */
  async save (fileName) {
    if (fileName.includes('.gz')) {
      await fs.promises.writeFile(fileName, zlib.gzipSync(this.toString(), {
        level: zlib.constants.Z_BEST_COMPRESSION
      }));
    } else {
      await fs.promises.writeFile(fileName, this.toString());
    }
  } /* CCOIndex::save() */

  /* ----------------------------------------------------------------------- */

  /**
   *
   */
  search (query, limit = 10, normalize = true) {
    if (!this._isOptimized) {
      this.optimize();
    }

    if ('object' !== typeof query) {
      console.error('Query should be an object with fields.');
      return [];
    }

    let hits = {};
    for (let [ field, fieldQuery ] of Object.entries(query)) {
      /* Make sure this field is indexed... */
      const internedField = this._intern(field, false);
      if (null === internedField || !(Object.hasOwnProperty.call(this._index,
        internedField))) {

        console.error(`Field ${field} is not indexed.`);
        continue;
      }

      field = internedField;

      const terms = this._tokenize(fieldQuery);

      for (let [ _, term ] of Object.entries(terms)) {
        /* If this term wasn't interned, it's not in the index... */
        if (null === term) {
          continue;
        }

        /* If this term isn't in the index for this field, on to the next... */
        if (!(Object.hasOwnProperty.call(this._index[`${field}`], term))) {
          continue;
        }

        /*
         * NOTE: While this map gives us O(1) updating of the document and score,
         *       a potential optimization *could* be to instead intern the docs
         *       locally and keep an array of our result objects, which would
         *       allow us to simply do an array-based-sort rather than the heap.
         *       This would introduce two lookups for each document, but for a
         *       large number of results, the sort may be faster to find the
         *       top-n.
         */
        for (let [ docId, docScore ] of Object.entries(
          this._index[`${field}`][`${term}`])) {

          if (!(Object.hasOwnProperty.call(hits, docId))) {
            hits[`${docId}`] = docScore;
          } else {
            hits[`${docId}`] += docScore;
          }
        }
      }
    }

    let heap = new Heap(function (a, b) {
      return a.score - b.score;
    });
    for (let [ docId, score ] of Object.entries(hits)) {
      if (!isNaN(score) && score > 0) {
        if (heap.size < limit) {
          heap.push({ id: docId, score: score });
        } else if (heap.peek().score < score) {
          heap.replace({ id: docId, score: score });
        }
      }
    }

    let results = heap.consume();
    results.sort(function(a, b) { return b.score - a.score; });
    for (let ii = 0; ii < results.length; ++ii) {
      results[ii].id = this._unintern(results[ii].id);
    }

    if (normalize) {
      let sum = 0;
      for (let [ _, result ] of Object.entries(results)) {
        sum += result.score;
      }
      for (let [ _, result ] of Object.entries(results)) {
        result.score = (result.score / sum);
      }
    }

    return results;
  } /* CCOIndex::search() */

  /* ======================================================================= */
  /* -- PRIVATE METHODS ---------------------------------------------------- */
  /* ======================================================================= */

  /**
   *
   */
  _intern (text, shouldAdd = true) {
    if (this._shouldIntern) {
      if (!(Object.hasOwnProperty.call(this._dictionary, text))) {
        if (shouldAdd) {
          let id = this._id++;
          this._dictionary[`${text}`] = String(id);
          this._strings.push(text);
        } else {
          return null;
        }
      }

      return this._dictionary[`${text}`];
    } else {
      return text;
    }
  } /* CCOIndex::_intern() */

  /* ----------------------------------------------------------------------- */

  /**
   *
   */
  _tokenize (input, shouldIntern = false) {
    let tokens = null;
    if (Array.isArray(input)) {
      tokens = input;
    } else {
      tokens = input.split(',');
    }

    let internedTokens = [];
    for (const [ _, token ] of Object.entries(tokens)) {
      internedTokens.push(this._intern(token, shouldIntern));
    }
    return internedTokens;
  } /* CCOIndex::_tokenize() */

  /* ----------------------------------------------------------------------- */

  /**
   *
   */
  _unintern (id) {
    id = parseInt(id);
    if (id < this._strings.length) {
      return this._strings[id];
    }
  } /* CCOIndex::_unintern() */

} /* CCOIndex() */

module.exports = function ccoindex (options) {
  return new CCOIndex(options)
};
