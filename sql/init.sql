DROP TABLE Accounts;

CREATE TABLE IF NOT EXISTS Accounts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    amount INT
);

INSERT INTO Accounts (amount) VALUES (100), (200), (300), (400);