package ee.ut.cs.dsg.d2ia.esper;

public class PersonEvent {
    private String name;
    private int age;
    private int id;


    public int getId() {
        return id;
    }

    public PersonEvent(String name, int age, int id) {
        this.name = name;
        this.age = age;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }
}