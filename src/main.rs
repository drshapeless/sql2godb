use convert_case::{Case, Casing};

#[derive(Default, Clone)]
struct Pair {
    key: String,
    value: String,
    gokey: String,
    govalue: String,
}

#[derive(Default)]
struct GoStruct {
    sql_type_name: String,
    go_type_name: String,
    members: Vec<Pair>,
}

impl Pair {
    fn to_godb(&mut self) {
        if self.key == "id" {
            self.gokey = self.key.to_case(Case::Upper);
        } else {
            self.gokey = self.key.to_case(Case::Pascal);
            self.gokey = self.gokey.replace("Id", "ID");
        }

        // bool is bool
        match self.value.as_str() {
            "bigserial" => self.govalue = "int64".into(),
            "bigint" => self.govalue = "int64".into(),
            "int" => self.govalue = "int32".into(),
            "text" => self.govalue = "string".into(),
            "timestamp(0)" => self.govalue = "time.Time".into(),
            "uuid" => self.govalue = "pgxuuid.uuid".into(),
            "float" => self.govalue = "float32".into(),
            _ => self.govalue = self.value.clone(),
        }
    }
}

impl GoStruct {
    fn make_go_type(&self) -> String {
        let mut s = String::new();

        s += &format!("type {} struct {{\n", self.go_type_name);
        for member in &self.members {
            s += &format!(
                "\t{} {} `db:\"{}\"`\n",
                member.gokey, member.govalue, member.key
            )
        }

        s += "}\n";

        s
    }

    fn make_go_create(&self) -> String {
        let mut s = String::new();

        s += &format!(
            "func Create{}({} *{}, db DB) error {{\n",
            self.go_type_name,
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name
        );

        s += "\tq := `INSERT INTO users (";

        s += &self
            .members
            .iter()
            .filter(|m| should_create(&m.key))
            .map(|m| m.key.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        s += ")\n";

        s += "VALUES (";

        s += &self
            .members
            .iter()
            .filter(|m| should_create(&m.key))
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect::<Vec<String>>()
            .join(", ");

        s += ")\n";
        s += "RETURNING id, version`\n\n";

        s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
\tdefer cancel()

";

        s += "\terr := db.QueryRow(ctx, q, ";

        s += &self
            .members
            .iter()
            .filter(|m| should_create(&m.key))
            .map(|m| {
                format!(
                    "{}.{}",
                    self.go_type_name.to_case(Case::Camel),
                    m.gokey
                )
            })
            .collect::<Vec<String>>()
            .join(", ");

        s += &format!(
            ").Scan(&{}.ID, &{}.Version)\n\n",
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name.to_case(Case::Camel)
        );

        s += "\tif err != nil {
\t\treturn err
\t}

\treturn nil
}
";

        s
    }

    fn make_go_get(&self) -> String {
        let mut s = String::new();

        s += &format!(
            "func Get{}(id {}, db DB) (*{}, error) {{\n",
            self.go_type_name,
            self.id_type(),
            self.go_type_name,
        );

        s += &format!(
            "\tq := `SELECT * FROM {} WHERE id = $1`\n\n",
            self.sql_type_name
        );

        s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
\tdefer cancel()

\trows, err := db.Query(ctx, q, id)
\tif err != nil {
\t\treturn nil, err
\t}

";

        s += &format!(
            "\t{}, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[{}])\n",
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name
        );

        s += "\tif err != nil {
\t\treturn nil, err
\t}

";

        s += "\treturn &";
        s += &self.go_type_name.to_case(Case::Camel);
        s += ", nil
}
";
        s
    }

    fn make_go_update(&self) -> String {
        let mut counter = 0;
        let mut s = String::new();

        s += &format!(
            "func Update{}({} *{}, db DB) error {{\n",
            self.go_type_name,
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name
        );

        s += &format!(
            "\tq := `UPDATE {}
SET ",
            self.sql_type_name
        );

        s += &self
            .members
            .iter()
            .filter(|m| should_create(&m.gokey))
            .enumerate()
            .map(|(i, m)| {
                counter = i.into();
                format!("{} = ${}, ", m.key, i + 1)
            })
            .collect::<Vec<String>>()
            .join("");

        s += "version = version + 1\n";

        s += &format!(
            "WHERE id = ${} AND version = ${}
RETURNING version`\n\n",
            counter + 2,
            counter + 3
        );

        s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
\tdefer cancel()

\terr := db.QueryRow(ctx, q, ";

        s += &self
            .members
            .iter()
            .filter(|m| should_create(&m.gokey))
            .map(|m| {
                format!(
                    "{}.{}",
                    self.go_type_name.to_case(Case::Camel),
                    m.gokey
                )
            })
            .collect::<Vec<String>>()
            .join(", ");

        s += &format!(
            ", {}.ID, {}.Version).Scan(&{}.Version)\n\n",
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name.to_case(Case::Camel),
            self.go_type_name.to_case(Case::Camel),
        );

        s += "\tif err != nil {
\t\treturn err
\t}

\treturn nil
}
";

        s
    }

    fn make_go_delete(&self) -> String {
        let mut s = String::new();

        s += &format!(
            "func Delete{}(id {}, db DB) error {{\n",
            self.go_type_name,
            self.id_type()
        );

        s += &format!(
            "\tq := `DELETE FROM {} WHERE id = $1`\n\n",
            self.sql_type_name
        );

        s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
\tdefer cancel()\n\n";

        s += "\tresult, err := db.Exec(ctx, q, id)
\tif err != nil {
\t\treturn err
\t}

\trowsAffected := result.RowsAffected()

\tif rowsAffected == 0 {
\t\treturn pgx.ErrNoRows
\t}

\treturn nil
}
";

        s
    }

    fn id_type(&self) -> String {
        for member in &self.members {
            if member.gokey == "ID" {
                return member.govalue.clone();
            }
        }

        return "".into();
    }
}

fn should_create(s: &str) -> bool {
    match s {
        "id" => false,
        "ID" => false,
        "version" => false,
        "Version" => false,
        "create_time" => false,
        "CreateTime" => false,
        "edit_time" => false,
        "EditTime" => false,
        _ => true,
    }
}

fn main() {
    let stdin = std::io::stdin();

    let mut started = false;
    let mut pairs: Vec<Pair> = Vec::new();
    let mut go_struct = GoStruct::default();

    for line in stdin.lines() {
        let l: String;
        match line {
            Ok(ll) => l = ll,
            Err(_) => break,
        }
        let line = l.trim();

        if line.starts_with("-") || line.is_empty() {
            continue;
        }

        let words: Vec<&str> = line.split(" ").collect();

        if words[0] == "CREATE" {
            let mut mytype: String = words[words.len() - 2].into();
            go_struct.sql_type_name = mytype.clone();

            match mytype.strip_suffix("s") {
                Some(s) => mytype = s.into(),
                None => {}
            }

            mytype = mytype.to_case(Case::Pascal);
            go_struct.go_type_name = mytype.clone();

            started = true;
            continue;
        }

        if words[0] == ");" {
            started = false;
            for pair in &mut pairs {
                pair.to_godb();
            }

            go_struct.members = pairs.clone();

            println!("{}", go_struct.make_go_type());

            println!("{}", go_struct.make_go_create());
            println!("{}", go_struct.make_go_get());
            println!("{}", go_struct.make_go_update());
            println!("{}", go_struct.make_go_delete());

            go_struct = GoStruct::default();
            pairs = Vec::new();
            continue;
        }

        if started {
            let mut pair = Pair::default();
            pair.key = words[0].into();
            pair.value = words[1].into();

            pairs.push(pair);
        }
    }
}
