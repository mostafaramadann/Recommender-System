import 'package:flutter/material.dart';
import 'package:flutter/cupertino.dart';
import 'package:flutter/widgets.dart';
import 'main.dart';

class AddCourses extends StatefulWidget {
  AddCourses({Key key}) : super(key: key);

  @override
  AddCoursesState createState() => AddCoursesState();
}

class AddCoursesState extends State<AddCourses> {
  final List<TextEditingController> controllers = new List();
  final _id1 = TextEditingController();

  @override
  void initState() {
    super.initState();
    controllers.add(_id1);
  }

  void addCourse(){

  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
        appBar: CupertinoNavigationBar(
          middle: Text(
            'Add Courses',
            style: TextStyle(color: Colors.teal),
          ),
        ),
        body: SafeArea(
            child: ListView(
                children: <Widget>[
                  new Column(
                    crossAxisAlignment: CrossAxisAlignment.center,
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: courses
                        .map((element) => Padding(
                          padding: EdgeInsets.fromLTRB(25, 40, 25, 0),
                          child: CupertinoTextField(
                            style: TextStyle(
                                fontSize: 18,
                                color: Theme.of(context).brightness == Brightness.dark ?
                                Colors.white : Colors.black
                            ),
                            placeholder: "Course ID",
                            controller: _id1,
                            prefix: Icon(
                              Icons.credit_card,
                              color: Colors.deepPurple,
                            ),
                          ),
                        )
                    ).toList()
                  ),
                  new Padding(
                    padding: EdgeInsets.fromLTRB(20, 80, 20, 30),
                    child: SizedBox(
                      width: double.infinity,
                      child: CupertinoButton(
                          borderRadius: BorderRadius.all(Radius.circular(4)),
                          minSize: 35,
                          padding: EdgeInsets.all(0),
                          color: Colors.deepPurple,
                          child: Text(
                            'Save',
                            style: TextStyle(
                              color: Colors.white,
                              fontSize: 16,
                            ),
                          ),
                          onPressed: () {
                            if (_id1.text.isEmpty) {
                              showDialog(
                                  context: context,
                                  builder: (BuildContext context) =>
                                      CupertinoAlertDialog(
                                        title: Text("Invalid"),
                                        content:
                                        Text("Please type in course ID"),
                                        actions: <Widget>[
                                          CupertinoDialogAction(
                                            isDefaultAction: true,
                                            child: Text("Got it"),
                                            onPressed: () {
                                              Navigator.of(context).pop();
                                            },
                                          )
                                        ],
                                      )
                              );
                            } else {
                              setState(() {
                                courses.add(_id1.text);
                              });

                              //ADD COURSE TO VISIBLE LIST
                            }
                          }
                      ),
                    ),
                  ),
                ]
            )
        ),
        floatingActionButton: FloatingActionButton(
        backgroundColor: Colors.teal,
        onPressed: () {

        },
        child: Icon(
          Icons.add,
          color: Colors.white,
        ),
      ),
    );
  }
}