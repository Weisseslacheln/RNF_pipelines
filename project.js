let project = {
  system_orgs: [
    {
      $group: {
        _id: "$ru.full_org",
        id: {
          $push: "$_id",
        },
      },
    },
    {
      $lookup: {
        from: "publ_rnf",
        localField: "id",
        foreignField: "id",
        as: "publ_rnf",
      },
    },
    {
      $set: {
        publ_rnf: {
          $filter: {
            input: "$publ_rnf",
            as: "el",
            cond: "$$el.eid",
          },
        },
      },
    },
    {
      $lookup: {
        from: "materials_scopus",
        localField: "publ_rnf.eid",
        foreignField: "_id",
        as: "publ_rnf",
      },
    },
    {
      $set: {
        publ_rnf: {
          $map: {
            input: "$publ_rnf",
            as: "el",
            in: "$$el.affiliation",
          },
        },
      },
    },
    {
      $set: {
        publ_rnf: {
          $reduce: {
            input: "$publ_rnf",
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
        publ_rnf_: {
          $reduce: {
            input: "$publ_rnf",
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
        publ_rnf: {
          $sortArray: {
            input: {
              $map: {
                input: "$publ_rnf_",
                as: "el",
                in: {
                  id: "$$el.id",
                  name: "$$el.name",
                  city: "$$el.city",
                  country: "$$el.country",
                  kol: {
                    $size: {
                      $filter: {
                        input: "$publ_rnf",
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
        publ_rnf_: "$delete",
      },
    },
    {
      $out: "system_orgs",
    },
  ],
};
