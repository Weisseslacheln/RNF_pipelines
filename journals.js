let journals = {
  "add-journal": [
    {
      $addFields: {
        journal: {
          $arrayElemAt: [
            {
              $split: ["$publ.info", ","],
            },
            0,
          ],
        },
        "system.journal": {
          $function: {
            body: function (name) {
              return name.toLowerCase().replaceAll(/[^a-zа-яё]/g, "");
            },
            args: ["$journal"],
            lang: "js",
          },
        },
      },
    },
    {
      $project: {
        journal: 1,
        "system.journal": 1,
      },
    },
    {
      $merge: "publ_rnf",
    },
  ],
  system_journals: [
    {
      $group: {
        _id: "$system.journal",
        ru: {
          $push: {
            $cond: [
              {
                $eq: ["$lang", "ru"],
              },
              "$id",
              "$delete",
            ],
          },
        },
        en: {
          $push: {
            $cond: [
              {
                $eq: ["$lang", "en"],
              },
              "$id",
              "$delete",
            ],
          },
        },
      },
    },
    {
      $addFields: {
        kol: {
          $sum: [
            {
              $size: "$ru",
            },
            {
              $size: "$en",
            },
          ],
        },
      },
    },
    {
      $sort: {
        kol: -1,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  journals2022: [
    {
      $set: {
        journals2022: {
          $function: {
            body: function (name) {
              return name.toLowerCase().replaceAll(/[^a-z]/g, "");
            },
            args: ["$_id"],
            lang: "js",
          },
        },
      },
    },
    {
      $lookup: {
        from: "journals2022",
        localField: "journals2022",
        foreignField: "system.clear_title",
        as: "journals2022",
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  "add-publ_rnf": [
    {
      $match: {
        _id: {
          $ne: "-",
        },
      },
    },
    {
      $lookup: {
        from: "publ_rnf",
        localField: "ru",
        foreignField: "id",
        as: "publ_rnf_ru",
      },
    },
    {
      $lookup: {
        from: "publ_rnf",
        localField: "en",
        foreignField: "id",
        as: "publ_rnf_en",
      },
    },
    {
      $set: {
        publ_rnf_ru: {
          $filter: {
            input: "$publ_rnf_ru",
            cond: {
              $eq: ["$$el.lang", "ru"],
            },
            as: "el",
          },
        },
        publ_rnf_en: {
          $filter: {
            input: "$publ_rnf_en",
            cond: {
              $eq: ["$$el.lang", "en"],
            },
            as: "el",
          },
        },
      },
    },
    {
      $addFields: {
        publ_rnf: {
          ru: {
            $cond: [
              {
                $not: {
                  $eq: [
                    {
                      $size: "$publ_rnf_ru",
                    },
                    0,
                  ],
                },
              },
              {
                count: {
                  $size: "$publ_rnf_ru",
                },
                eid: {
                  $size: "$publ_rnf_ru.eid",
                },
                eid_proc: {
                  $multiply: [
                    100,
                    {
                      $divide: [
                        {
                          $size: "$publ_rnf_ru.eid",
                        },
                        {
                          $size: "$publ_rnf_ru",
                        },
                      ],
                    },
                  ],
                },
                year: {
                  $size: {
                    $filter: {
                      input: "$publ_rnf_ru.year",
                      as: "el",
                      cond: {
                        $not: {
                          $eq: ["$$el", ""],
                        },
                      },
                    },
                  },
                },
                year_proc: {
                  $multiply: [
                    100,
                    {
                      $divide: [
                        {
                          $size: {
                            $filter: {
                              input: "$publ_rnf_ru.year",
                              as: "el",
                              cond: {
                                $not: {
                                  $eq: ["$$el", ""],
                                },
                              },
                            },
                          },
                        },
                        {
                          $size: "$publ_rnf_ru",
                        },
                      ],
                    },
                  ],
                },
              },
              "$delete",
            ],
          },
          en: {
            $cond: [
              {
                $not: {
                  $eq: [
                    {
                      $size: "$publ_rnf_en",
                    },
                    0,
                  ],
                },
              },
              {
                count: {
                  $size: "$publ_rnf_en",
                },
                eid: {
                  $size: "$publ_rnf_en.eid",
                },
                eid_proc: {
                  $multiply: [
                    100,
                    {
                      $divide: [
                        {
                          $size: "$publ_rnf_en.eid",
                        },
                        {
                          $size: "$publ_rnf_en",
                        },
                      ],
                    },
                  ],
                },
                year: {
                  $size: {
                    $filter: {
                      input: "$publ_rnf_en.year",
                      as: "el",
                      cond: {
                        $not: {
                          $eq: ["$$el", ""],
                        },
                      },
                    },
                  },
                },
                year_proc: {
                  $multiply: [
                    100,
                    {
                      $divide: [
                        {
                          $size: {
                            $filter: {
                              input: "$publ_rnf_en.year",
                              as: "el",
                              cond: {
                                $not: {
                                  $eq: ["$$el", ""],
                                },
                              },
                            },
                          },
                        },
                        {
                          $size: "$publ_rnf_en",
                        },
                      ],
                    },
                  ],
                },
              },
              "$delete",
            ],
          },
        },
      },
    },
    {
      $project: {
        publ_rnf: 1,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  news: [
    {
      $match: {
        scopus: null,
        "publ_rnf.ru.year_proc": 0,
        "publ_rnf.en.year_proc": 0,
      },
    },
    {
      $addFields: {
        news: true,
      },
    },
    {
      $project: {
        news: 1,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  "news+": [
    {
      $match: {
        en: [],
        news: null,
        "publ_rnf.ru.year_proc": 0,
      },
    },
    {
      $addFields: {
        news: true,
      },
    },
    {
      $project: {
        news: 1,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  conference: [
    {
      $match: {
        _id: new RegExp("conference"),
      },
    },
    {
      $addFields: {
        conference: true,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  "conference+": [
    {
      $match: {
        _id: new RegExp("онференци"),
      },
    },
    {
      $addFields: {
        conference: true,
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  "add-rnf": [
    {
      $match: {
        _id: {
          $ne: "-",
        },
      },
    },
    {
      $lookup: {
        from: "publ_rnf",
        localField: "ru",
        foreignField: "id",
        as: "publ_rnf_ru",
      },
    },
    {
      $lookup: {
        from: "publ_rnf",
        localField: "en",
        foreignField: "id",
        as: "publ_rnf_en",
      },
    },
    {
      $set: {
        publ_rnf_ru: {
          $filter: {
            input: "$publ_rnf_ru",
            cond: {
              $eq: ["$$el.lang", "ru"],
            },
            as: "el",
          },
        },
        publ_rnf_en: {
          $filter: {
            input: "$publ_rnf_en",
            cond: {
              $eq: ["$$el.lang", "en"],
            },
            as: "el",
          },
        },
      },
    },
    {
      $project: {
        publ_rnf_ru: "$publ_rnf_ru.eid",
        publ_rnf_en: "$publ_rnf_en.eid",
      },
    },
    {
      $set: {
        publ_rnf_ru: {
          $reduce: {
            input: "$publ_rnf_ru",
            initialValue: [],
            in: {
              $cond: [
                {
                  $eq: [
                    {
                      $indexOfArray: ["$$value", "$$this"],
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
        publ_rnf_en: {
          $reduce: {
            input: "$publ_rnf_en",
            initialValue: [],
            in: {
              $cond: [
                {
                  $eq: [
                    {
                      $indexOfArray: ["$$value", "$$this"],
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
      $lookup: {
        from: "materials_scopus",
        localField: "publ_rnf_ru",
        foreignField: "_id",
        as: "publ_rnf_ru",
      },
    },
    {
      $lookup: {
        from: "materials_scopus",
        localField: "publ_rnf_en",
        foreignField: "_id",
        as: "publ_rnf_en",
      },
    },
    {
      $set: {
        publ_rnf_ru: {
          $map: {
            input: "$publ_rnf_ru",
            as: "el",
            in: {
              _id: "$$el._id",
              source: "$$el.source",
              sourceid: "$$el.sourceid",
            },
          },
        },
        publ_rnf_en: {
          $map: {
            input: "$publ_rnf_en",
            as: "el",
            in: {
              _id: "$$el._id",
              source: "$$el.source",
              sourceid: "$$el.sourceid",
            },
          },
        },
      },
    },
    {
      $set: {
        publ_rnf: {
          $reduce: {
            input: {
              $concatArrays: ["$publ_rnf_ru", "$publ_rnf_en"],
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
      $project: {
        rnf: {
          publ_ru: "$publ_rnf_ru",
          publ_en: "$publ_rnf_en",
          publ: "$publ_rnf",
          source: {
            $reduce: {
              input: "$publ_rnf",
              initialValue: [],
              in: {
                $cond: [
                  {
                    $eq: [
                      {
                        $indexOfArray: ["$$value", "$$this.source"],
                      },
                      -1,
                    ],
                  },
                  {
                    $concatArrays: ["$$value", ["$$this.source"]],
                  },
                  "$$value",
                ],
              },
            },
          },
          sourceid: {
            $reduce: {
              input: "$publ_rnf",
              initialValue: [],
              in: {
                $cond: [
                  {
                    $eq: [
                      {
                        $indexOfArray: ["$$value", "$$this.sourceid"],
                      },
                      -1,
                    ],
                  },
                  {
                    $concatArrays: ["$$value", ["$$this.sourceid"]],
                  },
                  "$$value",
                ],
              },
            },
          },
        },
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  scopus_id_journals_anton_title_crossref: [
    {
      $lookup: {
        from: "journals_anton",
        localField: "_id",
        foreignField: "system.clear_title_crossref",
        as: "scopus_id_journals_anton_title_crossref",
      },
    },
    {
      $project: {
        scopus_id_journals_anton_title_crossref:
          "$scopus_id_journals_anton_title_crossref.id_scopus",
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  scopus_id_journals_anton_title_wl: [
    {
      $lookup: {
        from: "journals_anton",
        localField: "_id",
        foreignField: "system.clear_title_wl",
        as: "scopus_id_journals_anton_title_wl",
      },
    },
    {
      $project: {
        scopus_id_journals_anton_title_wl:
          "$scopus_id_journals_anton_title_wl.id_scopus",
      },
    },
    {
      $merge: "system_journals",
    },
  ],
  "add-scopus": [
    {
      $addFields: {
        scopus: {
          $cond: [
            {
              $not: {
                $eq: ["$journals2022", []],
              },
            },
            "$journals2022._id",
            "$delete",
          ],
        },
      },
    },
    {
      $set: {
        scopus: {
          $cond: [
            {
              $and: [
                {
                  $not: "$scopus",
                },
                {
                  $not: {
                    $eq: ["$rnf.sourceid", []],
                  },
                },
              ],
            },
            "$rnf.sourceid",
            "$scopus",
          ],
        },
      },
    },
    {
      $set: {
        scopus: {
          $cond: [
            {
              $and: [
                {
                  $not: "$scopus",
                },
                {
                  $not: {
                    $eq: [
                      {
                        $concatArrays: [
                          "$scopus_id_journals_anton_title_crossref",
                          "$scopus_id_journals_anton_title_wl",
                        ],
                      },
                      [],
                    ],
                  },
                },
              ],
            },
            {
              $concatArrays: [
                "$scopus_id_journals_anton_title_crossref",
                "$scopus_id_journals_anton_title_wl",
              ],
            },
            "$scopus",
          ],
        },
      },
    },
    {
      $merge: "system_journals",
    },
  ],
};
