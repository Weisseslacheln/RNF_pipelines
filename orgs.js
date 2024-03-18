let org = {
  "add-aff": [
    {
      $lookup: {
        from: "6.scopus",
        localField: "publ_rnf.id",
        foreignField: "scopus_id",
        as: "aff",
      },
    },
    {
      $set: {
        aff: {
          $sortArray: {
            input: {
              $map: {
                input: "$aff",
                as: "el",
                in: {
                  scopus_id: "$$el.scopus_id",
                  code: "$$el.code",
                  kol: {
                    $getField: {
                      field: "kol",
                      input: {
                        $arrayElemAt: [
                          {
                            $filter: {
                              input: "$publ_rnf",
                              as: "elem",
                              cond: {
                                $eq: ["$$elem.id", "$$el.scopus_id"],
                              },
                            },
                          },
                          0,
                        ],
                      },
                    },
                  },
                },
              },
            },
            sortBy: {
              kol: -1,
            },
          },
        },
      },
    },
    {
      $lookup: {
        from: "1-2",
        localField: "aff.code",
        foreignField: "code",
        as: "aff_",
      },
    },
    {
      $set: {
        aff: {
          $sortArray: {
            input: {
              $map: {
                input: "$aff_",
                as: "el",
                in: {
                  scopus_id: {
                    $getField: {
                      field: "scopus_id",
                      input: {
                        $arrayElemAt: [
                          {
                            $filter: {
                              input: "$aff",
                              as: "elem",
                              cond: {
                                $eq: ["$$elem.code", "$$el.code"],
                              },
                            },
                          },
                          0,
                        ],
                      },
                    },
                  },
                  code: "$$el.code",
                  name_full: "$$el.name_full",
                  name_short: "$$el.name_short",
                  name_full_system: {
                    $function: {
                      body: function (name) {
                        return name.toLowerCase().replaceAll(/[^a-zа-яё]/g, "");
                      },
                      args: ["$$el.name_full"],
                      lang: "js",
                    },
                  },
                  kol: {
                    $getField: {
                      field: "kol",
                      input: {
                        $arrayElemAt: [
                          {
                            $filter: {
                              input: "$aff",
                              as: "elem",
                              cond: {
                                $eq: ["$$elem.code", "$$el.code"],
                              },
                            },
                          },
                          0,
                        ],
                      },
                    },
                  },
                },
              },
            },
            sortBy: {
              kol: -1,
            },
          },
        },
        aff_: "$delete",
        _id_system: {
          $function: {
            body: function (name) {
              return name.toLowerCase().replaceAll(/[^a-zа-яё]/g, "");
            },
            args: ["$_id"],
            lang: "js",
          },
        },
      },
    },
    {
      $merge: "system_orgs",
    },
  ],
  "add-scopus_id": [
    {
      $match: {
        aff: {
          $ne: [],
        },
      },
    },
    {
      $addFields: {
        check: {
          $function: {
            body: function (id, aff) {
              return aff.map((el) => {
                return {
                  scopus_id: el.scopus_id,
                  id_aff: el.name_full_system.indexOf(id),
                  aff_id: id.indexOf(el.name_full_system),
                };
              });
            },
            args: ["$_id_system", "$aff"],
            lang: "js",
          },
        },
      },
    },
    {
      $addFields: {
        scopus_id: {
          $getField: {
            field: "scopus_id",
            input: {
              $arrayElemAt: [
                {
                  $filter: {
                    input: "$check",
                    as: "el",
                    cond: {
                      $and: [
                        {
                          $eq: ["$$el.id_aff", 0],
                        },
                        {
                          $eq: ["$$el.aff_id", 0],
                        },
                      ],
                    },
                  },
                },
                0,
              ],
            },
          },
        },
      },
    },
    {
      $merge: "system_orgs",
    },
  ],
  cosine: [
    {
      $project: {
        cosine: {
          $map: {
            input: "$aff",
            as: "el",
            in: {
              scopus_id: "$$el.scopus_id",
              name_full: "$$el.name_full",
              kol: "$$el.kol",
              kol_number: {
                $sum: [
                  {
                    $indexOfArray: ["$publ_rnf.id", "$$el.scopus_id"],
                  },
                  1,
                ],
              },
              kol_perc: {
                $divide: [
                  {
                    $multiply: ["$$el.kol", 100],
                  },
                  {
                    $reduce: {
                      input: "$publ_rnf",
                      in: {
                        $sum: ["$$value", "$$this.kol"],
                      },
                      initialValue: 0,
                    },
                  },
                ],
              },
              cosine: {
                $function: {
                  body: function (text1, text2) {
                    function wordCountMap(str) {
                      let words = str
                        .toLowerCase()
                        .replaceAll(/[ *]/g, " ")
                        .replaceAll(/[^a-zа-яё ]/g, "")
                        .trim()
                        .split(" ");
                      let wordCount = {};
                      words.forEach((w) => {
                        wordCount[w] = (wordCount[w] || 0) + 1;
                      });
                      return wordCount;
                    }
                    function addWordsToDictionary(wordCountmap, dict) {
                      for (let key in wordCountmap) {
                        dict[key] = true;
                      }
                    }
                    function wordMapToVector(map, dict) {
                      let wordCountVector = [];
                      for (let term in dict) {
                        wordCountVector.push(map[term] || 0);
                      }
                      return wordCountVector;
                    }
                    function dotProduct(vecA, vecB) {
                      let product = 0;
                      for (let i = 0; i < vecA.length; i++) {
                        product += vecA[i] * vecB[i];
                      }
                      return product;
                    }
                    function magnitude(vecA, vecB) {
                      let sumA = 0;
                      let sumB = 0;
                      for (let i = 0; i < vecA.length; i++) {
                        sumA += vecA[i] * vecA[i];
                      }
                      for (let i = 0; i < vecB.length; i++) {
                        sumB += vecB[i] * vecB[i];
                      }
                      return Math.sqrt(sumA * sumB);
                    }
                    function cosineSimilarity(vecA, vecB) {
                      return dotProduct(vecA, vecB) / magnitude(vecA, vecB);
                    }
                    function textCosineSimilarity(txtA, txtB) {
                      const wordCountA = wordCountMap(txtA);
                      const wordCountB = wordCountMap(txtB);
                      let dict = {};
                      addWordsToDictionary(wordCountA, dict);
                      addWordsToDictionary(wordCountB, dict);
                      const vectorA = wordMapToVector(wordCountA, dict);
                      const vectorB = wordMapToVector(wordCountB, dict);
                      return cosineSimilarity(vectorA, vectorB);
                    }
                    return textCosineSimilarity(text1, text2);
                  },
                  args: ["$_id", "$$el.name_full"],
                  lang: "js",
                },
              },
            },
          },
        },
      },
    },
    {
      $set: {
        cosine: {
          $sortArray: {
            input: "$cosine",
            sortBy: {
              cosine: -1,
            },
          },
        },
      },
    },
    {
      $merge: "system_orgs",
    },
  ],
  "add-eid": [
    {
      $match: {
        scopus_id: null,
      },
    },
    {
      $match: {
        $expr: {
          $gt: [
            {
              $size: "$id",
            },
            1,
          ],
        },
      },
    },
    {
      $lookup: {
        from: "projects",
        localField: "id",
        foreignField: "_id",
        as: "eid",
      },
    },
    {
      $project: {
        eid: {
          $reduce: {
            input: "$eid",
            initialValue: [],
            in: {
              $concatArrays: ["$$value", "$$this.eid"],
            },
          },
        },
      },
    },
    {
      $lookup: {
        from: "materials_scopus",
        localField: "eid",
        foreignField: "_id",
        as: "publ_rnf",
      },
    },
    {
      $lookup: {
        from: "scopus_funding",
        localField: "eid",
        foreignField: "_id",
        as: "publ_rnf_",
      },
    },
    {
      $project: {
        eid: {
          $reduce: {
            input: {
              $concatArrays: ["$publ_rnf", "$publ_rnf_"],
            },
            initialValue: [],
            in: {
              $cond: [
                {
                  $eq: [
                    {
                      $indexOfArray: ["$$value._id", "$$this._id"],
                    },
                    -1,
                  ],
                },
                {
                  $concatArrays: ["$$value", ["$$this"]],
                },
                "$$value",
              ],
            },
          },
        },
      },
    },
    {
      $set: {
        eid: {
          $map: {
            input: "$eid",
            as: "el",
            in: "$$el.affiliation",
          },
        },
      },
    },
    {
      $set: {
        eid: {
          $reduce: {
            input: "$eid",
            initialValue: [],
            in: {
              $concatArrays: ["$$value", "$$this"],
            },
          },
        },
      },
    },
    {
      $set: {
        eid_: {
          $reduce: {
            input: "$eid",
            initialValue: [],
            in: {
              $cond: [
                {
                  $cond: [
                    "$$value.id",
                    {
                      $in: ["$$this.id", "$$value.id"],
                    },
                    false,
                  ],
                },
                "$$value",
                {
                  $concatArrays: [
                    "$$value",
                    [
                      {
                        id: "$$this.id",
                        name: "$$this.name",
                        city: "$$this.city",
                        country: "$$this.country",
                      },
                    ],
                  ],
                },
              ],
            },
          },
        },
      },
    },
    {
      $set: {
        eid: {
          $sortArray: {
            input: {
              $map: {
                input: "$eid_",
                as: "el",
                in: {
                  id: "$$el.id",
                  name: "$$el.name",
                  city: "$$el.city",
                  country: "$$el.country",
                  kol: {
                    $size: {
                      $filter: {
                        input: "$eid",
                        as: "elem",
                        cond: {
                          $eq: ["$$elem.id", "$$el.id"],
                        },
                      },
                    },
                  },
                },
              },
            },
            sortBy: {
              kol: -1,
            },
          },
        },
        eid_: "$delete",
      },
    },
    {
      $merge: "system_orgs",
    },
  ],
  "add-_id_en": [
    {
      $match: {
        scopus_id: null,
      },
    },
    {
      $lookup: {
        from: "projects",
        localField: "id",
        foreignField: "_id",
        as: "res",
      },
    },
    {
      $project: {
        _id_en: {
          $map: {
            input: "$res",
            as: "el",
            in: "$$el.en.Affiliation",
          },
        },
      },
    },
    {
      $set: {
        _id_en: {
          $reduce: {
            input: "$_id_en",
            initialValue: [],
            in: {
              $cond: {
                if: {
                  $and: [
                    {
                      $eq: [
                        {
                          $indexOfArray: ["$$value", "$$this"],
                        },
                        -1,
                      ],
                    },
                    {
                      $not: {
                        $eq: ["$$this", null],
                      },
                    },
                  ],
                },
                then: {
                  $concatArrays: ["$$value", ["$$this"]],
                },
                else: "$$value",
              },
            },
          },
        },
      },
    },
    {
      $merge: "system_orgs",
    },
  ],
};
