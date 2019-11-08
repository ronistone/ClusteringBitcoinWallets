from sklearn import tree

X = [[0,0], [0,1]]
Y = [0, 1]


classifier = tree.DecisionTreeClassifier()

classifier.fit(X, Y)

print(classifier.predict([[0,2]]))


X = [[1,0], [2,0]]
Y = [0, 1]


classifier = tree.DecisionTreeClassifier()

classifier.fit(X, Y)

print(classifier.predict([[0,2]]))


X = [[0,0], [0,1], [1,0], [2,0]]
Y = [0, 1, 0, 1]


classifier = tree.DecisionTreeClassifier()

classifier.fit(X, Y)

print(classifier.predict([[0,2]]))
