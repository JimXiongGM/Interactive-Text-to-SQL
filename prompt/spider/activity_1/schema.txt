| Table | Primary Key | Foreign Key | Row Count |
| --- | --- | --- | --- |
| Activity | actid |  | 16 |
| Faculty | FacID |  | 58 |
| Faculty_Participates_in |  | actid references Activity(actid), FacID references Faculty(FacID) | 40 |
| Participates_in |  | actid references Activity(actid), stuid references Student(StuID) | 70 |
| Student | StuID |  | 34 |
