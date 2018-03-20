namespace java thrift

struct JavaEETechnologyVersions {
    1: string versionForJava4;
    2: string versionForJava5;
    3: string versionForJava6;
    4: string versionForJava7;
    5: string versionForJava8;
}

struct JavaEETechnology {
    1: i32 id;
    2: string name;
    3: JavaEETechnologyVersions versions;
    4: string description;
}

service J2EeHandbookService {
    list<JavaEETechnology> getAllTechnologies();
    void addTechnology(1: JavaEETechnology technology);
    void removeTechnology(1: JavaEETechnology technology);
    void updateTechnology(1:JavaEETechnology technology);
}
