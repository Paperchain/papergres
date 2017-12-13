CREATE SCHEMA paper;

CREATE TABLE paper.book(
	book_id 	BIGSERIAL 		PRIMARY KEY,
	title 		TEXT 			NOT NULL,
	author 		TEXT 			NOT NULL,
	created_by	VARCHAR(255)	NOT NULL,
	created_at	TIMESTAMP  		DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT uc_book_title UNIQUE(title)
);

CREATE TABLE paper.character(
	character_id 	BIGSERIAL 		PRIMARY KEY,
	book_id 		BIGSERIAL	 	REFERENCES paper.book(book_id),
	name 			TEXT 			NOT NULL,
	description 	TEXT 			NOT NULL,
	created_by		VARCHAR(255)	NOT NULL,
	created_at		TIMESTAMP  		DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO paper.book(title, author, created_by)
VALUES('Dune', 'Frank Herbert', 'create-tables.sql');

INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(1, 'Pual Atreides', 'He was the one', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(1, 'Jessica Atreides', 'The resolute opposer who gave birth to the one and the abomination', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(1, 'Duncan Idaho', 'Swordmaster of the Ginaz and loyal servant of the Atreides', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(1, 'Chani', 'Freeman wife of Paul Ateides', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(1, 'Vladimir Harkonnen', 'The baddie', 'create-tables.sql');

INSERT INTO paper.book(title, author, created_by)
VALUES('Ready Player One', 'Earnest Cline', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(2, 'Wade Watts aka Parzival', 'The hero and savior of the Oasis', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(2, 'Art3mis', 'Love infatuation of Parzival and excellent Gunter', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(2, 'Aech', 'BFF of Parzival', 'create-tables.sql');

INSERT INTO paper.book(title, author, created_by)
VALUES('Hyperion', 'Dan Simmons', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(3, 'The Consul', 'Traitor to the Hegemony but necessary participant on the final pilgrimage', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(3, 'Colonel Fedmahn Kassad', 'Badass military guy falls in love with Moneta in his dreams and battles the Shrike', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(3, 'The Shrike', 'The baddie. 9 foot tall time travelling spikey monster machine', 'create-tables.sql');

INSERT INTO paper.book(title, author, created_by)
VALUES('Pandora''s Star', 'Peter F Hamilton', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(4, 'Wilson Kime', 'Navy Commander of the first interstellar ship to the Prime twins', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(4, 'Ozzie', 'Eccentric co-inventor of wormhole technology and founder of the Commonwealth', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(4, 'MorningLightMountain', 'Baddie alien, leader of the Prime who are bent on destroying humanity', 'create-tables.sql');

INSERT INTO paper.book(title, author, created_by)
VALUES('Leviathan Wakes', 'James S. A. Corey', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(5, 'James Holden', 'Slacker turned hero who becomes the Captain of the Rocinante and warrior against evil', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(5, 'Amos', 'Any scene with Amos is a good scene', 'create-tables.sql');
INSERT INTO paper.character(book_id, name, description, created_by)
VALUES(5, 'Avasarala', 'She speaks, uh, frankly', 'create-tables.sql');